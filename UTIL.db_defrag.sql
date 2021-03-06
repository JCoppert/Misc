USE [HALOCOREDB]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		Jordan Coppert
-- Create date: 06/05/2017
-- Description:	Defragments all tables in a specified database by rebuilding indexes which 
--              have a fragmentation level greater than a specified threshold, or reorganize
--				indexes based on an OPTIONAL argument. To configure the database change the 
--				above USE statment, and specify the threshold level as an argument during 
--				invocation.
-- =============================================
ALTER PROCEDURE [UTIL].[db_defrag] 

	/* Arguments indicate 5 total digits in the decimal, to the left and right of the decimal,
	  and 3 digits to the right of the decimal. Formally known as precision and scale. */
	@Threshold decimal(5, 3),

	/* Specify lower limit of index reorganization, reorganize if 
	  ( lowerLim < defrag% < Threshold), if value != NULL reorganization is toggled ON */
	@ReorganizeLowerLimit decimal (5, 3) = NULL

AS
BEGIN

	SET NOCOUNT ON;
	DECLARE @objectid int;
	DECLARE @indexid int;
	DECLARE @partitioncount bigint;  
	DECLARE @schemaname nvarchar(130);   
	DECLARE @objectname nvarchar(130);   
	DECLARE @indexname nvarchar(130);   
	DECLARE @partitionnum bigint;  
	DECLARE @frag float;  
	DECLARE @command nvarchar(4000);  
	DECLARE @command2 nvarchar(4000);
	DECLARE @action nvarchar(11) = N'REBUILD';
	DECLARE @indexCount int = 0;
	DECLARE @tableCount int;
	DECLARE @totalFragmentation float = 0.00;
	DECLARE @totalFragmentationPercentReduction float = 0.00;
	DECLARE @postActionFragmentationValue float;
	DECLARE @result nvarchar(4000);

	/* FOR TESTING TO TARGET THE FIRST RETURNED TABLE
	DECLARE @increment int;
    SET @increment = 0;*/ 
	
	/*
	-Retrieve pertinent defrag information from MS stored 
	 procedure sys.dm_db_index_physical_stats
	-Arguments for given procedure are as follows: target current database 
	 (DB_ID()), NULL to interrogate all tables and views in the DB, NULL to 
	 interrogate all indexes, NULL to interrogate all partitions, and DEFAULT
	 for limited statistics. Configure as desired.
	-Results are put into a temp table
	*/
	SELECT object_id AS objectid, index_id AS indexid, partition_number AS partitionnum,  
		avg_fragmentation_in_percent AS frag
		INTO #indexes_for_defrag
		FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, DEFAULT)
	-- ****UNCOMMENT TO LIMIT SEARCH RESULTS OF ABOVE QUERY
	--WHERE avg_fragmentation_in_percent >= @Threshold;

	--Open cursor for temp table iteration
	DECLARE IndexesCursor CURSOR FOR SELECT * FROM #indexes_for_defrag;
	OPEN IndexesCursor;

	--For statistics
	IF @ReorganizeLowerLimit IS NOT NULL 
		SELECT @tableCount = COUNT(DISTINCT objectid) FROM #indexes_for_defrag
		WHERE (frag >= @ReorganizeLowerLimit);
	ELSE
		SELECT @tableCount = COUNT(DISTINCT objectid) FROM #indexes_for_defrag
		WHERE (frag > @Threshold);

	--Make the while condition @increment < 1 for testing
	WHILE(@@FETCH_STATUS = 0)
	BEGIN
		
		FETCH NEXT FROM IndexesCursor INTO @objectid, @indexid, @partitionnum, @frag;

		/*
		-Is reorganized toggled and is the fragmentation enough enough to consider
		 acting on it? If not is the fragmentation at least equal to the threshold
		 for rebuilding it. If neither condition is met, don't act on the index.
		*/ 
		IF (@ReorganizeLowerLimit IS NOT NULL AND @frag < @ReorganizeLowerLimit) OR
			(@ReorganizeLowerLimit IS NULL AND @frag < @Threshold) BREAK;

		/*
		-Get the schema and table names to initialize local variables, 
		 QUOTENAME casts object values as strings.
	    -Access the set of user defined, schema-scoped objects, 
		 within a database 
		-Inner join entries between objects and schemas (want correct 
		 cross product to match on schema_id column)
	    -Require that included object rows have the objectid we are looking 
		 for which is the next entry in the 
		 cursor (objectid in the cursor row corresponds to table name)
		*/
		SELECT @objectname = QUOTENAME(o.name), @schemaname = QUOTENAME(s.name) 
			FROM sys.objects AS o  
			JOIN sys.schemas as s ON s.schema_id = o.schema_id  
			WHERE o.object_id = @objectid;  

		/* 
		-Get the index to initialize local variable
		-Require that included index rows have correct table (@objectid) 
		 and index (@indexid) values
		*/
        SELECT @indexname = QUOTENAME(name)  
			FROM sys.indexes  
			WHERE  object_id = @objectid AND index_id = @indexid;  

		/*
		-Get the number of partitions for later recreation
		-Require that included partition rows have correct 
		 table (@objectid) and index (@indexid) values
		*/
        SELECT @partitioncount = count (*)  
			FROM sys.partitions  
			WHERE object_id = @objectid AND index_id = @indexid;

		-- Determine appropriate action to take
		IF @ReorganizeLowerLimit IS NOT NULL 
			IF @frag < @Threshold AND @frag > @ReorganizeLowerLimit
				SET @action = N'REORGANIZE';
		
		-- Build dynamic query string
		SET @command = N'ALTER INDEX' + @indexname + N' ON ' + @schemaname + 
			N'.' + @objectname + @action;

		-- Reorganize is always performed online, if command is REBUILD it must be toggled
		IF @ReorganizeLowerLimit IS NULL
			SET @command = @command + N' WITH (ONLINE = ON)';

		/* When acting on the index you must also take into consideration existing 
			partitions */
		IF @partitioncount > 1
			SET @command = @command + N' PARTITION=' + CAST(@partitionnum AS nvarchar(10));
		
		-- Execute rebuild or reorganization
		EXEC (@command);

		-- Update associated index statistics
		SET @command2 = 'UPDATE STATISTICS ' + @schemaname + N'.' + @objectname + ' ' 
			+ @indexname;
		EXEC (@command2);

		-- Output and bookkeeping
		PRINT N'Updated statistics on' + @objectname + N' and executed ' + @command;
		PRINT N'Fragmentation before ' + @action + N' was ' + Str(@frag) + N'%';
		PRINT '----------------------------------------------------------------------'
		SET @indexCount = @indexCount + 1;
			
		--Populate temp table to gather fragmentation after action taken
		SELECT object_id AS objectid, index_id AS indexid, partition_number AS pn, avg_fragmentation_in_percent AS frag
			INTO #mostRecentlyUpdated
			FROM sys.dm_db_index_physical_stats(DB_ID(), @objectid, @indexid, NULL, DEFAULT)
			WHERE object_id = @objectid AND index_id = @indexid AND partition_number = @partitionnum;
			
		/*Get post action value of fragmentation, I believe this to work. Values are different when I query the fragmentation
		of the index following the stored procedure. I suspect this to be to traffic intercepting the table before I can run my
		query
		*/
		SELECT @postActionFragmentationValue = COUNT(frag) FROM #mostRecentlyUpdated; 
			
		--Arithmetic
		SET @totalFragmentationPercentReduction = @totalFragmentationPercentReduction 
			+ (@frag - @postActionFragmentationValue);

		PRINT STR(@postActionFragmentationValue);

		--Deallocate
		DROP TABLE #mostRecentlyUpdated;


		--FOR TESTING (see above):   
		--SET @increment = 1;
		

		SET @totalFragmentation = @totalFragmentation + @frag;

	END;

	--Summary, uncomment for Messages window output, other output is direct through email
	PRINT N'Summary';
	PRINT N'Indexes affected: ' + Str(@indexCount);
	PRINT N'Distinct tables affected: ' + Str(@tableCount);
	PRINT N'Average database index fragmentation: ' + Str((@totalFragmentation / @indexCount));
	PRINT N'Average fragmentation reduction on indexes: ' + 
		Str((@totalFragmentationPercentReduction / @indexCount));

	SET @result = N'Indexes affected: ' + Str(@indexCount) + N'\r\n
	 Distinct tables affected: ' + Str(@tableCount) + N'\r\n
	 Average database index fragmentation: ' + Str((@totalFragmentation / @indexCount)) + N'\r\n 
	 Average fragmentation reduction on indexes: ' + 
		Str((@totalFragmentationPercentReduction / @indexCount));

	-- Deallocate resources
	CLOSE IndexesCursor;
	DEALLOCATE IndexesCursor;
	DROP TABLE #indexes_for_defrag;

	--Email output, I need a valid Database Mail profile in order for this to work
	/*EXEC msdb.dbo.sp_send_dbmail
	@profile_name = 'Jordan Coppert',
	@recipients = 'jordan.coppert@houseadv.com',
	@subject = 'Database Defragmentation Report',
	@body = @result;*/

END;


