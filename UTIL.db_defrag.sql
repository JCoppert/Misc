
--CHANGE THIS USE STATEMENT IN ORDER TO CONFIGURE THE DB VALUE
USE HALOCOREDB;
GO
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
CREATE PROCEDURE UTIL.db_defrag 
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
	DECLARE @partitions bigint;  
	DECLARE @frag float;  
	DECLARE @command nvarchar(4000);  
	DECLARE @CurrentIndex nvarchar(100);

	--DB_ID() will return the ID of the current database, change use statement above in accordance with 
	/*--Desired result set
	SELECT DB_NAME(database_id) as Current_Database_Name, Object_Name(object_id) as Table_Name, index_type_desc, 
	avg_fragmentation_in_percent, fragment_count, avg_fragment_size_in_pages, page_count 
	INTO #indexes_for_defrag
	FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, DEFAULT)
	WHERE avg_fragmentation_in_percent > @Threshold; */

	SELECT object_id AS objectid, index_id AS indexid, partition_number AS partitionnum,  
	avg_fragmentation_in_percent AS frag
	INTO #indexes_for_defrag
	FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, DEFAULT)
	WHERE avg_fragmentation_in_percent >= @Threshold;

	DECLARE IndexesCursor CURSOR FOR SELECT * FROM #indexes_for_defrag;
	OPEN IndexesCursor;

	WHILE(1=1)
	BEGIN
		
		FETCH NEXT FROM IndexesCursor INTO @objectid, @indexid, @partitionnum, @frag;
		IF @@FETCH_STATUS < 0 BREAK;
		SELECT @objectname = QUOTENAME(o.name), @schemaname = QUOTENAME(s.name)  
        FROM sys.objects AS o  
        JOIN sys.schemas as s ON s.schema_id = o.schema_id  
        WHERE o.object_id = @objectid;  
        SELECT @indexname = QUOTENAME(name)  
        FROM sys.indexes  
        WHERE  object_id = @objectid AND index_id = @indexid;  
        SELECT @partitioncount = count (*)  
        FROM sys.partitions  
        WHERE object_id = @objectid AND index_id = @indexid;  

		SET @command = N'ALTER INDEX' + @indexname + N' ON ' + @schemaname+ N'.' + @objectname + N' REBUILD';
		PRINT  @command;
	END;
		
	CLOSE IndexesCursor;
	DEALLOCATE IndexesCursor;
	DROP TABLE #indexes_for_defrag;
END;
GO

/*
		EXEC @command;
		UPDATE STATISTICS @schemaname.@objectName;

		PRINT N'Executed ' + @command;
	END;

	CLOSE IndexesCursor;
	DEALLOCATE IndexesCursor;
	DROP TABLE #indexes_for_defrag;

END;
GO*/



	/*DECLARE DBTables CURSOR FOR 
	SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
	WHERE TABLE_TYPE = 'BASE TABLE'

	OPEN DBTables

	FETCH NEXT FROM DBTables into @TableName

	WHILE(@@FETCH_STATUS = 0)
	BEGIN
		
		--Populate result set of indexes 
		DECLARE TableKeys CURSOR FOR 
		SELECT * FROM sys.indexes 
		WHERE object_id = (SELECT object_id FROM sys.objects
			WHERE name = @TableName)
		
		WHILE(@@FETCH_STATUS = 0)
		BEGIN
			
			SELECT * FROM sys.dm_db_index_physical_stats(DB_ID(), OBJECT_ID(N
			


	END */
-- END
-- GO
