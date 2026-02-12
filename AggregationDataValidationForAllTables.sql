/*============================================================================
FULL SCRIPT: Bronze vs Silver metric comparison for MULTIPLE tables (hardcoded list)

Schemas:
  - Bronze: bronze_salesforce
  - Silver: silver_salesforce

Metrics rules (per column):
  - String-like columns   -> <Col>_Populated = count rows where trimmed value <> ''
  - Date/time columns     -> <Col>_Populated = count rows where value > '1900-01-01'
  - Numeric/bit columns   -> <Col>_Sum       = sum(col) with NULL -> 0
  - Always includes TotalRows

Exclusions:
  - PCBronzeBatchId, PCDataHash, PCVersionDatetime
  - AND any column starting with 'PC' (PC%)

Robustness:
  - If a table is missing in one schema, that side’s metrics return NULL
  - STRING_AGG forced to NVARCHAR(MAX) to avoid 8000-byte truncation
============================================================================*/

SET NOCOUNT ON;

DECLARE
    @BronzeSchema SYSNAME = N'bronze_salesforce',
    @SilverSchema SYSNAME = N'silver_salesforce',
    @TableName    SYSNAME,
    @SQL          NVARCHAR(MAX);

-- Tables to run (from your screenshot)
DECLARE @Tables TABLE (TableName SYSNAME PRIMARY KEY);
INSERT INTO @Tables (TableName)
VALUES
 (N'Account'),
 (N'Contact'),
 (N'CountrieswithIncomeLevel__c'),
 (N'npe5__Affiliation__c'),
 (N'OrderApi__Renewal__c'),
 (N'OrderApi__Business_Group__c'),
 (N'OrderApi__EPayment__c'),
 (N'OrderApi__EPayment_Line__c'),
 (N'OrderApi__GL_Account__c'),
 (N'OrderApi__Item__c'),
 (N'OrderApi__Item_Class__c'),
 (N'OrderApi__Known_Address__c'),
 (N'OrderApi__Payment_Gateway__c'),
 (N'OrderApi__Payment_Method__c'),
 (N'OrderApi__Payment_Terms__c'),
 (N'OrderApi__Receipt__c'),
 (N'OrderApi__Receipt_Line__c'),
 (N'OrderApi__Subscription__c'),
 (N'OrderApi__Sales_Order__c'),
 (N'RecordType'),
 (N'OrderApi__Source_Code__c'),
 (N'OrderApi__Subscription_Line__c'),
 (N'OrderApi__Sales_Order_Line__c'),
 (N'User2');

-- Output table (all results)
IF OBJECT_ID('tempdb..#AllMetricResults') IS NOT NULL DROP TABLE #AllMetricResults;
CREATE TABLE #AllMetricResults
(
    TableName    SYSNAME,
    Metric       SYSNAME,
    BronzeValue  DECIMAL(38,10) NULL,
    SilverValue  DECIMAL(38,10) NULL,
    Diff         DECIMAL(38,10) NULL,
    RunDtm       DATETIME DEFAULT GETDATE()
);

-- Working metadata table (per table)
IF OBJECT_ID('tempdb..#Metrics') IS NOT NULL DROP TABLE #Metrics;
CREATE TABLE #Metrics
(
    ColumnName     SYSNAME NOT NULL,
    MetricName     SYSNAME NOT NULL,
    Category       VARCHAR(10) NOT NULL,   -- STRING | DATE | NUM
    ExistsInBronze BIT NOT NULL,
    ExistsInSilver BIT NOT NULL,
    PRIMARY KEY (MetricName)
);

DECLARE c CURSOR LOCAL FAST_FORWARD FOR
SELECT TableName FROM @Tables ORDER BY TableName;

OPEN c;
FETCH NEXT FROM c INTO @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT '============================================================';
    PRINT 'Processing table: ' + @TableName;
    PRINT '============================================================';

    TRUNCATE TABLE #Metrics;

    DECLARE @BronzeExists BIT =
        CASE WHEN OBJECT_ID(QUOTENAME(@BronzeSchema) + N'.' + QUOTENAME(@TableName)) IS NOT NULL THEN 1 ELSE 0 END;

    DECLARE @SilverExists BIT =
        CASE WHEN OBJECT_ID(QUOTENAME(@SilverSchema) + N'.' + QUOTENAME(@TableName)) IS NOT NULL THEN 1 ELSE 0 END;

    ;WITH b AS (
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = @BronzeSchema AND TABLE_NAME = @TableName
          AND COLUMN_NAME NOT IN ('PCBronzeBatchId','PCDataHash','PCVersionDatetime')
          AND COLUMN_NAME NOT LIKE 'PC%'
    ),
    s AS (
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = @SilverSchema AND TABLE_NAME = @TableName
          AND COLUMN_NAME NOT IN ('PCBronzeBatchId','PCDataHash','PCVersionDatetime')
          AND COLUMN_NAME NOT LIKE 'PC%'
    ),
    u AS (
        SELECT
            COALESCE(s.COLUMN_NAME, b.COLUMN_NAME) AS ColumnName,
            COALESCE(s.DATA_TYPE,  b.DATA_TYPE)    AS DataType,
            CASE WHEN b.COLUMN_NAME IS NULL THEN 0 ELSE 1 END AS ExistsInBronze,
            CASE WHEN s.COLUMN_NAME IS NULL THEN 0 ELSE 1 END AS ExistsInSilver
        FROM b
        FULL OUTER JOIN s
            ON s.COLUMN_NAME = b.COLUMN_NAME
    )
    INSERT INTO #Metrics (ColumnName, MetricName, Category, ExistsInBronze, ExistsInSilver)
    SELECT
        ColumnName,
        CAST(
            ColumnName +
            CASE
                WHEN DataType IN ('bit','tinyint','smallint','int','bigint','decimal','numeric','money','smallmoney','float','real')
                    THEN '_Sum'
                ELSE '_Populated'
            END
            AS SYSNAME
        ) AS MetricName,
        CASE
            WHEN DataType IN ('bit','tinyint','smallint','int','bigint','decimal','numeric','money','smallmoney','float','real')
                THEN 'NUM'
            WHEN DataType IN ('date','datetime','datetime2','smalldatetime','datetimeoffset','time')
                THEN 'DATE'
            ELSE 'STRING'
        END AS Category,
        CAST(ExistsInBronze AS BIT),
        CAST(ExistsInSilver AS BIT)
    FROM u
    WHERE ColumnName IS NOT NULL;

    DECLARE @BronzeMetricUnion NVARCHAR(MAX) = N'';
    DECLARE @SilverMetricUnion NVARCHAR(MAX) = N'';
    DECLARE @AllMetricNamesUnion NVARCHAR(MAX) = N'';

    -- Build Bronze tall metrics UNION ALL (each SELECT includes FROM bronze.table)
    SELECT @BronzeMetricUnion =
        STRING_AGG(
            CAST(
                N'SELECT N''' + REPLACE(MetricName,'''','''''') + N''' AS Metric, ' +
                N'CAST(' +
                CASE Category
                    WHEN 'NUM' THEN
                        N'SUM(ISNULL(CAST(' + QUOTENAME(ColumnName) + N' AS DECIMAL(38,10)),0))'
                    WHEN 'DATE' THEN
                        N'SUM(CASE WHEN ' + QUOTENAME(ColumnName) + N' IS NOT NULL AND ' + QUOTENAME(ColumnName) + N' > ''1900-01-01'' THEN 1 ELSE 0 END)'
                    ELSE
                        N'SUM(CASE WHEN NULLIF(LTRIM(RTRIM(CAST(' + QUOTENAME(ColumnName) + N' AS NVARCHAR(MAX)))),'''') IS NOT NULL THEN 1 ELSE 0 END)'
                END +
                N' AS DECIMAL(38,10)) AS Value ' +
                N'FROM ' + QUOTENAME(@BronzeSchema) + N'.' + QUOTENAME(@TableName)
            AS NVARCHAR(MAX))
        , NCHAR(10) + N'UNION ALL' + NCHAR(10))
    FROM #Metrics
    WHERE ExistsInBronze = 1;

    -- Build Silver tall metrics UNION ALL (each SELECT includes FROM silver.table)
    SELECT @SilverMetricUnion =
        STRING_AGG(
            CAST(
                N'SELECT N''' + REPLACE(MetricName,'''','''''') + N''' AS Metric, ' +
                N'CAST(' +
                CASE Category
                    WHEN 'NUM' THEN
                        N'SUM(ISNULL(CAST(' + QUOTENAME(ColumnName) + N' AS DECIMAL(38,10)),0))'
                    WHEN 'DATE' THEN
                        N'SUM(CASE WHEN ' + QUOTENAME(ColumnName) + N' IS NOT NULL AND ' + QUOTENAME(ColumnName) + N' > ''1900-01-01'' THEN 1 ELSE 0 END)'
                    ELSE
                        N'SUM(CASE WHEN NULLIF(LTRIM(RTRIM(CAST(' + QUOTENAME(ColumnName) + N' AS NVARCHAR(MAX)))),'''') IS NOT NULL THEN 1 ELSE 0 END)'
                END +
                N' AS DECIMAL(38,10)) AS Value ' +
                N'FROM ' + QUOTENAME(@SilverSchema) + N'.' + QUOTENAME(@TableName)
            AS NVARCHAR(MAX))
        , NCHAR(10) + N'UNION ALL' + NCHAR(10))
    FROM #Metrics
    WHERE ExistsInSilver = 1;

    -- Build master metric-name list for final output (includes TotalRows)
    ;WITH m AS (
        SELECT MetricName FROM #Metrics
        UNION ALL
        SELECT 'TotalRows'
    )
    SELECT @AllMetricNamesUnion =
        STRING_AGG(
            CAST(N'SELECT N''' + REPLACE(MetricName,'''','''''') + N''' AS Metric' AS NVARCHAR(MAX))
        , NCHAR(10) + N'UNION ALL' + NCHAR(10))
    FROM m;

    -- Compose dynamic SQL per table
    SET @SQL = N'
/* Metrics comparison: [' + @BronzeSchema + N'].[' + @TableName + N'] vs [' + @SilverSchema + N'].[' + @TableName + N'] */

WITH AllMetrics AS (
' + @AllMetricNamesUnion + N'
),
BronzeMetrics AS (
' + CASE
        WHEN @BronzeExists = 1 THEN
            N'SELECT N''TotalRows'' AS Metric, CAST(COUNT(*) AS DECIMAL(38,10)) AS Value
              FROM ' + QUOTENAME(@BronzeSchema) + N'.' + QUOTENAME(@TableName) + N'
              UNION ALL
              ' + ISNULL(@BronzeMetricUnion, N'SELECT N''__none__'' AS Metric, CAST(NULL AS DECIMAL(38,10)) AS Value WHERE 1=0')
        ELSE
            N'SELECT Metric, CAST(NULL AS DECIMAL(38,10)) AS Value FROM AllMetrics'
    END + N'
),
SilverMetrics AS (
' + CASE
        WHEN @SilverExists = 1 THEN
            N'SELECT N''TotalRows'' AS Metric, CAST(COUNT(*) AS DECIMAL(38,10)) AS Value
              FROM ' + QUOTENAME(@SilverSchema) + N'.' + QUOTENAME(@TableName) + N'
              UNION ALL
              ' + ISNULL(@SilverMetricUnion, N'SELECT N''__none__'' AS Metric, CAST(NULL AS DECIMAL(38,10)) AS Value WHERE 1=0')
        ELSE
            N'SELECT Metric, CAST(NULL AS DECIMAL(38,10)) AS Value FROM AllMetrics'
    END + N'
)
SELECT
    N''' + REPLACE(@TableName,'''','''''') + N''' AS TableName,
    a.Metric,
    b.Value AS BronzeValue,
    s.Value AS SilverValue,
    CAST((ISNULL(s.Value,0) - ISNULL(b.Value,0)) AS DECIMAL(38,10)) AS Diff
FROM AllMetrics a
LEFT JOIN BronzeMetrics b ON a.Metric = b.Metric
LEFT JOIN SilverMetrics s ON a.Metric = s.Metric
ORDER BY a.Metric;
';

    INSERT INTO #AllMetricResults (TableName, Metric, BronzeValue, SilverValue, Diff)
    EXEC sys.sp_executesql @SQL;

    FETCH NEXT FROM c INTO @TableName;
END

CLOSE c;
DEALLOCATE c;

-- Final output: all tables, all metrics
SELECT *
FROM #AllMetricResults
ORDER BY TableName, Metric;

SET NOCOUNT OFF;
