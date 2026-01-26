-- USE aan-dev-dw-app-sqldb-ncus;  -- or Salesforce_Prod;

------------------------------------------------------------
-- 1) Parameters
------------------------------------------------------------
DECLARE @schema      sysname = 'bronze_salesforce';
DECLARE @ChunkSize   int     = 10000;
DECLARE @CutoffDate  date    = '2026-01-20';

------------------------------------------------------------
-- 2) Table list
------------------------------------------------------------
DROP TABLE IF EXISTS #tables_to_hash;
CREATE TABLE #tables_to_hash (TableName sysname NOT NULL);

INSERT INTO #tables_to_hash (TableName)
VALUES
 ('Account'),
 ('Contact'),
 ('CountrieswithIncomeLevel__c'),
 ('npe5__Affiliation__c'),
 ('OrderApi__Renewal__c'),
 ('OrderApi__Business_Group__c'),
 ('OrderApi__EPayment__c'),
 ('OrderApi__EPayment_Line__c'),
 ('OrderApi__GL_Account__c'),
 ('OrderApi__Item__c'),
 ('OrderApi__Item_Class__c'),
 ('OrderApi__Known_Address__c'),
 ('OrderApi__Payment_Gateway__c'),
 ('OrderApi__Payment_Method__c'),
 ('OrderApi__Payment_Terms__c'),
 ('OrderApi__Receipt__c'),
 ('OrderApi__Receipt_Line__c'),
 ('OrderApi__Subscription__c'),
 ('OrderApi__Sales_Order__c'),
 ('OrderApi__Sales_Order_Line__c'),
 ('OrderApi__Source_Code__c'),
 ('OrderApi__Subscription_Line__c'),
 ('RecordType'),
 ('User2');

------------------------------------------------------------
-- 3) Output table
------------------------------------------------------------
DROP TABLE IF EXISTS #table_hash_output;

CREATE TABLE #table_hash_output (
    SchemaName              sysname,
    TableName               sysname,
    ColumnCount_All         int,
    ColumnCount_Hashed      int,
    table_value_hash        varbinary(32),
    row_count               bigint,
    distinct_row_hash_count bigint,
    ErrorMessage            nvarchar(4000),
    RunDtmUtc               datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME()
);

------------------------------------------------------------
-- 4) Column counts
------------------------------------------------------------
DROP TABLE IF EXISTS #table_column_counts;

CREATE TABLE #table_column_counts (
    SchemaName         sysname,
    TableName          sysname,
    ColumnCount_All    int,
    ColumnCount_Hashed int
);

INSERT INTO #table_column_counts
SELECT
    s.name,
    t.name,
    COUNT(*) AS ColumnCount_All,
    SUM(CASE WHEN UPPER(c.name) NOT IN ('ID', 'PCBATCHID') THEN 1 ELSE 0 END)
FROM #tables_to_hash tl
JOIN sys.tables  t ON t.name = tl.TableName
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN sys.columns c ON c.object_id = t.object_id
WHERE UPPER(s.name) = UPPER(@schema)
GROUP BY s.name, t.name;

------------------------------------------------------------
-- 5) Loop tables and compute hash (WITH DATE FILTER)
------------------------------------------------------------
DECLARE @table     sysname;
DECLARE @RowConcat nvarchar(max);
DECLARE @Sql       nvarchar(max);

DECLARE table_cursor CURSOR FAST_FORWARD FOR
SELECT TableName FROM #tables_to_hash ORDER BY TableName;

OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @table;

WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        SET @RowConcat = NULL;
        SET @Sql = NULL;

        SELECT @RowConcat =
            STRING_AGG(
                CONVERT(nvarchar(max),
                    'UPPER(LTRIM(RTRIM(COALESCE(CAST(' + QUOTENAME(c.name) +
                    ' AS nvarchar(4000)), ''''))))'
                ),
                N' + ''|'' + '
            ) WITHIN GROUP (ORDER BY c.column_id)
        FROM sys.columns c
        JOIN sys.tables  t ON c.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE UPPER(s.name) = UPPER(@schema)
          AND UPPER(t.name) = UPPER(@table)
          AND UPPER(c.name) NOT IN ('ID', 'PCBATCHID');

        IF @RowConcat IS NULL
        BEGIN
            INSERT INTO #table_hash_output (SchemaName, TableName, ErrorMessage)
            VALUES (@schema, @table, 'No columns found or all columns excluded.');
            FETCH NEXT FROM table_cursor INTO @table;
            CONTINUE;
        END;

        SET @Sql = N'
        ;WITH row_hashes AS (
            SELECT HASHBYTES(''SHA2_256'', ' + @RowConcat + N') AS row_hash
            FROM ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@table) + N'
            WHERE CAST(SystemModstamp AS date) < @CutoffDateParam
        ),
        ordered AS (
            SELECT row_hash, ROW_NUMBER() OVER (ORDER BY row_hash) AS rn
            FROM row_hashes
        ),
        chunked AS (
            SELECT (rn - 1) / ' + CAST(@ChunkSize AS nvarchar(20)) + N' AS chunk_id, rn, row_hash
            FROM ordered
        ),
        chunk_hashes AS (
            SELECT
                chunk_id,
                HASHBYTES(
                    ''SHA2_256'',
                    STRING_AGG(CONVERT(varchar(max), CONVERT(varchar(64), row_hash, 2)), '''')
                        WITHIN GROUP (ORDER BY rn)
                ) AS chunk_hash
            FROM chunked
            GROUP BY chunk_id
        )
        INSERT INTO #table_hash_output
            (SchemaName, TableName, table_value_hash, row_count, distinct_row_hash_count)
        SELECT
            @SchemaNameParam,
            @TableNameParam,
            HASHBYTES(
                ''SHA2_256'',
                STRING_AGG(CONVERT(varchar(max), CONVERT(varchar(64), chunk_hash, 2)), '''')
                    WITHIN GROUP (ORDER BY chunk_id)
            ),
            (SELECT COUNT(*) FROM row_hashes),
            (SELECT COUNT(DISTINCT row_hash) FROM row_hashes)
        FROM chunk_hashes;
        ';

        EXEC sys.sp_executesql
            @Sql,
            N'@SchemaNameParam sysname, @TableNameParam sysname, @CutoffDateParam date',
            @SchemaNameParam = @schema,
            @TableNameParam  = @table,
            @CutoffDateParam = @CutoffDate;

        UPDATE o
        SET
            o.ColumnCount_All    = cc.ColumnCount_All,
            o.ColumnCount_Hashed = cc.ColumnCount_Hashed
        FROM #table_hash_output o
        JOIN #table_column_counts cc
          ON cc.SchemaName = o.SchemaName
         AND cc.TableName  = o.TableName
        WHERE o.SchemaName = @schema
          AND o.TableName  = @table
          AND o.ErrorMessage IS NULL;

    END TRY
    BEGIN CATCH
        INSERT INTO #table_hash_output (SchemaName, TableName, ErrorMessage)
        VALUES (@schema, @table, ERROR_MESSAGE());
    END CATCH;

    FETCH NEXT FROM table_cursor INTO @table;
END

CLOSE table_cursor;
DEALLOCATE table_cursor;

------------------------------------------------------------
-- 6) Final results
------------------------------------------------------------
SELECT *
FROM #table_hash_output
ORDER BY
    CASE WHEN ErrorMessage IS NULL THEN 0 ELSE 1 END,
    TableName;
