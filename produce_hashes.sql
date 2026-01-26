-- USE aan-dev-dw-app-sqldb-ncus;

-- Temp table stores output of hashing SPROC
DROP TABLE IF EXISTS #hashed_output;

-- This temp table needs to match the table you're checking 
-- But exclude the "Id" and "PCBatchId" columns
-- And include the "row_hash" column as the first column on the table
-- Example based on CountriesWithIncomeLevel__c:
CREATE TABLE #hashed_output (
	row_hash VARBINARY(32)
	--, [Id] [char](18) NOT NULL
	, [OwnerId] [char](18) NULL
	, [IsDeleted] [bit] NULL
	, [Name] [nvarchar](80) NULL
	, [CreatedDate] [datetime2](3) NULL
	, [CreatedById] [char](18) NULL
	, [LastModifiedDate] [datetime2](3) NULL
	, [LastModifiedById] [char](18) NULL
	, [SystemModstamp] [datetime2](3) NULL
	, [LastActivityDate] [date] NULL
	, [LastViewedDate] [datetime2](3) NULL
	, [LastReferencedDate] [datetime2](3) NULL
	, [CountryName__c] [nvarchar](255) NULL
	, [IncomeLevel__c] [nvarchar](255) NULL
	, [Region__c] [nvarchar](255) NULL
	, [IsoCode__c] [nvarchar](3) NULL
	, [Iso2Code__c] [nvarchar](2) NULL
	, [BackupCreatedDate] [datetime2](3) NULL
	, [BackupModifiedDate] [datetime2](3) NULL
	--, [PCBatchId] [int] NOT NULL
	);

-- Change @schema and @table to match the table you're checking
DECLARE @schema nvarchar(max) = 'bronze_salesforce';
DECLARE @table nvarchar(max) = 'CountriesWithIncomeLevel__c';

DECLARE @column_list nvarchar(max);
DECLARE @sql         nvarchar(max);

SELECT @column_list =
    STRING_AGG(CAST(c.name AS nvarchar(max)), ', ')
        WITHIN GROUP (ORDER BY c.column_id)
FROM sys.columns c
JOIN sys.tables  t ON c.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE UPPER(s.name) = UPPER(@schema)
  AND UPPER(t.name) = UPPER(@table)
  AND UPPER(c.name) NOT IN ('ID', 'PCBATCHID');

SELECT @sql =
    STRING_AGG(
        CAST(
            'UPPER(LTRIM(RTRIM(COALESCE(CAST(' + QUOTENAME(c.name) +
            ' AS nvarchar(4000)), ''''))))'
            AS nvarchar(max)
        ),
        ' + ''|'' + '
    ) WITHIN GROUP (ORDER BY c.column_id)
FROM sys.columns c
JOIN sys.tables  t ON c.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE UPPER(s.name) = UPPER(@schema)
  AND UPPER(t.name) = UPPER(@table)
  AND UPPER(c.name) NOT IN ('ID', 'PCBATCHID');

SET @sql = '
    SELECT DISTINCT
        HASHBYTES(''SHA2_256'', ' + @sql + ') AS row_hash,
        ' + @column_list + '
    FROM ' + QUOTENAME(@schema) + '.' + QUOTENAME(@table) + ';';

INSERT INTO #hashed_output
EXEC sys.sp_executesql @sql;

-- Temp table now holds hashed output
-- Save it as a csv file in either the "source" or "target" directory
-- Name the csv file the same as the table
SELECT * FROM #hashed_output;