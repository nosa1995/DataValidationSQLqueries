
     /*============================================================================
 FULL SCRIPT (ADJUSTED): Bronze vs Silver metric comparison for ONE table (hardcoded)
 Table : [bronze_salesforce].[Contact] vs [silver_salesforce].[Contact]

 Excludes columns (if present in either side):
   - PCBronzeBatchId
   - PCDataHash
   - PCVersionDatetime

 Rules:
   - String-like columns   -> *_Populated = count rows where trimmed value <> ''
   - Date/time columns     -> *_Populated = count rows where value > '1900-01-01'
   - Numeric/bit columns   -> *_Sum       = sum of the column (NULL -> 0)
   - Always includes TotalRows

 Notes:
   - Uses metadata to auto-generate metrics for ALL columns (no manual list)
   - Compares columns that exist in either schema (if missing in one side,
     that side’s metric will be NULL)
   - FIX: STRING_AGG forced to NVARCHAR(MAX) to avoid 8000-byte truncation
============================================================================*/

SET NOCOUNT ON;

DECLARE
    @BronzeSchema SYSNAME = N'bronze_salesforce',
    @SilverSchema SYSNAME = N'silver_salesforce',
    @TableName    SYSNAME = N'Contact',
    @SQL          NVARCHAR(MAX);

IF OBJECT_ID('tempdb..#Cols') IS NOT NULL DROP TABLE #Cols;

CREATE TABLE #Cols
(
    ColumnName SYSNAME NOT NULL,
    MetricName SYSNAME NOT NULL,
    Category   VARCHAR(10) NOT NULL,  -- STRING | DATE | NUM
    PRIMARY KEY (ColumnName, MetricName)   -- <-- no named constraint (avoids collision)
);

;WITH b AS (
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @BronzeSchema AND TABLE_NAME = @TableName
),
s AS (
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @SilverSchema AND TABLE_NAME = @TableName
),
u AS (
    SELECT
        COALESCE(s.COLUMN_NAME, b.COLUMN_NAME) AS ColumnName,
        COALESCE(s.DATA_TYPE,  b.DATA_TYPE)    AS DataType
    FROM b
    FULL OUTER JOIN s
        ON s.COLUMN_NAME = b.COLUMN_NAME
)
INSERT INTO #Cols (ColumnName, MetricName, Category)
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
    END AS Category
FROM u
WHERE ColumnName IS NOT NULL
  AND ColumnName NOT IN ('PCBronzeBatchId','PCDataHash','PCVersionDatetime');  -- EXCLUDE

DECLARE @SilverSelect NVARCHAR(MAX);
DECLARE @BronzeSelect NVARCHAR(MAX);
DECLARE @ValuesList  NVARCHAR(MAX);

-- Build Silver aggregate select list (FORCE NVARCHAR(MAX) INSIDE STRING_AGG)
SELECT @SilverSelect =
    STRING_AGG(
        CAST(
            CASE Category
                WHEN 'NUM' THEN
                    N', SUM(ISNULL(CAST(' + QUOTENAME(ColumnName) + N' AS DECIMAL(38,10)), 0)) AS ' + QUOTENAME(MetricName)
                WHEN 'DATE' THEN
                    N', SUM(CASE WHEN ' + QUOTENAME(ColumnName) + N' IS NOT NULL AND ' + QUOTENAME(ColumnName) + N' > ''1900-01-01'' THEN 1 ELSE 0 END) AS ' + QUOTENAME(MetricName)
                ELSE
                    N', SUM(CASE WHEN NULLIF(LTRIM(RTRIM(CAST(' + QUOTENAME(ColumnName) + N' AS NVARCHAR(MAX)))), '''') IS NOT NULL THEN 1 ELSE 0 END) AS ' + QUOTENAME(MetricName)
            END
        AS NVARCHAR(MAX))
    , CHAR(10)) WITHIN GROUP (ORDER BY MetricName)
FROM #Cols;

-- Build Bronze aggregate select list (FORCE NVARCHAR(MAX) INSIDE STRING_AGG)
SELECT @BronzeSelect =
    STRING_AGG(
        CAST(
            CASE Category
                WHEN 'NUM' THEN
                    N', SUM(ISNULL(CAST(' + QUOTENAME(ColumnName) + N' AS DECIMAL(38,10)), 0)) AS ' + QUOTENAME(MetricName)
                WHEN 'DATE' THEN
                    N', SUM(CASE WHEN ' + QUOTENAME(ColumnName) + N' IS NOT NULL AND ' + QUOTENAME(ColumnName) + N' > ''1900-01-01'' THEN 1 ELSE 0 END) AS ' + QUOTENAME(MetricName)
                ELSE
                    N', SUM(CASE WHEN NULLIF(LTRIM(RTRIM(CAST(' + QUOTENAME(ColumnName) + N' AS NVARCHAR(MAX)))), '''') IS NOT NULL THEN 1 ELSE 0 END) AS ' + QUOTENAME(MetricName)
            END
        AS NVARCHAR(MAX))
    , CHAR(10)) WITHIN GROUP (ORDER BY MetricName)
FROM #Cols;

-- Build the (VALUES ...) list to output one row per metric (FORCE NVARCHAR(MAX))
;WITH m AS (
    SELECT MetricName
    FROM #Cols
    UNION ALL
    SELECT 'TotalRows'
)
SELECT @ValuesList =
    STRING_AGG(
        CAST(
            CASE WHEN MetricName = 'TotalRows' THEN
                N'(''TotalRows'', CAST(Bronze.TotalRows AS DECIMAL(38,10)), CAST(Silver.TotalRows AS DECIMAL(38,10)))'
            ELSE
                N'(''' + REPLACE(MetricName,'''','''''') + N''', ' +
                N'CAST(Bronze.' + QUOTENAME(MetricName) + N' AS DECIMAL(38,10)), ' +
                N'CAST(Silver.' + QUOTENAME(MetricName) + N' AS DECIMAL(38,10)))'
            END
        AS NVARCHAR(MAX))
    , N',' + CHAR(10)) WITHIN GROUP (ORDER BY MetricName)
FROM m;

SET @SQL = N'
/* Populated/Sum comparison: [' + @BronzeSchema + N'].[' + @TableName + N'] vs [' + @SilverSchema + N'].[' + @TableName + N'] */

WITH Silver AS (
    SELECT
        ''' + @SilverSchema + N'.' + @TableName + N''' AS TableName,
        COUNT(*) AS TotalRows
        ' + ISNULL(@SilverSelect, N'') + N'
    FROM ' + QUOTENAME(@SilverSchema) + N'.' + QUOTENAME(@TableName) + N'
),
Bronze AS (
    SELECT
        ''' + @BronzeSchema + N'.' + @TableName + N''' AS TableName,
        COUNT(*) AS TotalRows
        ' + ISNULL(@BronzeSelect, N'') + N'
    FROM ' + QUOTENAME(@BronzeSchema) + N'.' + QUOTENAME(@TableName) + N'
)
SELECT
    v.Metric,
    Bronze.TableName AS BronzeTable,
    Silver.TableName AS SilverTable,
    v.BronzeValue,
    v.SilverValue,
    (v.SilverValue - v.BronzeValue) AS Diff
FROM Bronze
CROSS JOIN Silver
CROSS APPLY (VALUES
' + ISNULL(@ValuesList, N'') + N'
) v(Metric, BronzeValue, SilverValue)
ORDER BY v.Metric;
';

EXEC sys.sp_executesql @SQL;

SET NOCOUNT OFF;
