USE [HALOCOREDB]
GO
/****** Object:  StoredProcedure [UTIL].[db_defrag]    Script Date: 6/6/2017 1:46:50 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		Jordan Coppert
-- Create date: 06/05/2017
-- Description:	Defragments all tables in a specified database by rebuilding indexes which have a fragmentation
--              level greater than a specified threshold. To configure the database change the above USE statment,
--              and specify the threshold level as an argument during invocation.
-- =============================================
ALTER PROCEDURE [UTIL].[db_defrag] 
	--Arguments indicate 5 total digits in the decimal, to the left and right of the decimal,
	-- and 3 digits to the right of the decimal. Formally known as precision and scale.
	@Threshold decimal(5, 3)

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

	/* FOR TESTING 
	DECLARE @increment int;
    SET @increment = 0; */
	

	SELECT object_id AS objectid, index_id AS indexid, partition_number AS partitionnum,  
	avg_fragmentation_in_percent AS frag
	INTO #indexes_for_defrag
	FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, DEFAULT)
	WHERE avg_fragmentation_in_percent >= @Threshold;

	DECLARE IndexesCursor CURSOR FOR SELECT * FROM #indexes_for_defrag;
	OPEN IndexesCursor;

	--Make the while condition @increment < 1 for testing
	WHILE(1=1)
	BEGIN
		
		FETCH NEXT FROM IndexesCursor INTO @objectid, @indexid, @partitionnum, @frag;
		IF @@FETCH_STATUS < 0 BREAK;

		--Get the schema and table names to initialize local variables, QUOTENAME casts object values as strings
		SELECT @objectname = QUOTENAME(o.name), @schemaname = QUOTENAME(s.name) 
		-- Access the set of user defined, schema-scoped objects, within a database 
        FROM sys.objects AS o  
		-- Inner join entries between objects and schemas (want correct cross product to match on schema_id column)
        JOIN sys.schemas as s ON s.schema_id = o.schema_id  
		-- Require that included object rows have the objectid we are looking for which is the next entry in the 
		-- cursor (objectid in the cursor row corresponds to table name)
        WHERE o.object_id = @objectid;  

		--Get the index to initialize local variable
        SELECT @indexname = QUOTENAME(name)  
        FROM sys.indexes  
		-- Require that included index rows have correct table (@objectid) and index (@indexid) values
        WHERE  object_id = @objectid AND index_id = @indexid;  

		--Get the number of partitions for later recreation
        SELECT @partitioncount = count (*)  
        FROM sys.partitions  
		-- Require that included partition rows have correct table (@objectid) and index (@indexid) values
        WHERE object_id = @objectid AND index_id = @indexid;  

		-- On allows the rebuilding to occur without blocking taking place on other queries
		SET @command = N'ALTER INDEX' + @indexname + N' ON ' + @schemaname+ N'.' + @objectname + N' REBUILD WITH (ONLINE = ON)';
		-- When rebuilding the index you must also take into consideration existing partitions
		IF @partitioncount > 1
			SET @command = @command + N' PARTITION=' + CAST(@partitionnum AS nvarchar(10));
		EXEC (@command);
		SET @command2 = 'UPDATE STATISTICS ' + @schemaname + N'.' + @objectname + ' ' + @indexname;
		EXEC (@command2);

		PRINT N'Updated statistics on' + @objectname + N' and executed ' + @command;
		PRINT N'Fragmentation before rebuild was ' + Str(@frag) + N'%';

		--FOR TESTING (see above):   
		--SET @increment = 1;
	END;
	
	-- Deallocate resources
	CLOSE IndexesCursor;
	DEALLOCATE IndexesCursor;
	DROP TABLE #indexes_for_defrag;
END;


