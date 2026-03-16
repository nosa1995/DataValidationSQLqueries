
-- DYNAMIC DATA VALIDATION SCRIPT - GROUPED BY TEST
-- Validates all tables between bronze_salesforce and silver_salesforce schemas
-- ============================================================================
-- Purpose: Automated validation of data migration from bronze to silver layer
-- Structure: Runs each validation test across ALL tables before moving to next test
-- Date: 2026-02-09
--
-- ============================================================================

SET NOCOUNT ON;

-- ============================================================================
-- Create temporary results table
-- ============================================================================
IF OBJECT_ID('tempdb..#TableValidationResults') IS NOT NULL
    DROP TABLE #TableValidationResults;

CREATE TABLE #TableValidationResults (
    TableName          SYSNAME,
    TestNumber         INT,
    TestName           VARCHAR(100),
    Result             VARCHAR(10),
    Details            NVARCHAR(MAX),

    -- Row Existence Check extra columns (populated only for that test)
    OnlyInBronzeCount  INT NULL,
    OnlyInSilverCount  INT NULL,
    BronzeOnlyIds      NVARCHAR(MAX) NULL,
    SilverOnlyIds      NVARCHAR(MAX) NULL,

    ExecutionTime      DATETIME DEFAULT GETDATE()
);

PRINT '============================================================================';
PRINT 'Dynamic Table Validation Script - Grouped by Test';
PRINT 'Bronze Schema: bronze_salesforce';
PRINT 'Silver Schema: silver_salesforce';
PRINT 'Execution Time: ' + CONVERT(VARCHAR(30), GETDATE(), 120);
PRINT '============================================================================';
PRINT '';

-- ============================================================================
-- Variables
-- ============================================================================
DECLARE @TableName SYSNAME;
DECLARE @SQL NVARCHAR(MAX);
DECLARE @TotalTables INT = 0;

-- ============================================================================
-- Hardcoded table list to validate
-- ============================================================================
DECLARE @TablesToValidate TABLE (TableName SYSNAME);
INSERT INTO @TablesToValidate VALUES
    ('Account'),
    ('Contact'),
    ('CountriesWithIncomeLevel__c'),
    ('npe5__Affiliation__c'),
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
    ('OrderApi__Renewal__c'),
    ('OrderApi__Sales_Order__c'),
    ('OrderApi__Sales_Order_Line__c'),
    ('OrderApi__Source_Code__c'),
    ('OrderApi__Subscription__c'),
    ('OrderApi__Subscription_Line__c'),
    ('RecordType'),
    ('User2');

SELECT @TotalTables = COUNT(*) FROM @TablesToValidate;

PRINT 'Found ' + CAST(@TotalTables AS VARCHAR) + ' tables to validate.';
PRINT 'Running 5 validation tests across all tables...';
PRINT '';

-- ============================================================================
-- TEST 1: COLUMN VALIDATION (Count + Missing Columns)
-- ============================================================================
PRINT '============================================================================';
PRINT 'TEST 1: COLUMN VALIDATION (Count + Missing Columns)';
PRINT '============================================================================';

DECLARE TableCursor1 CURSOR FAST_FORWARD FOR
SELECT TableName FROM @TablesToValidate ORDER BY TableName;

OPEN TableCursor1;
FETCH NEXT FROM TableCursor1 INTO @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT 'Processing: ' + @TableName;

    BEGIN TRY
        SET @SQL = N'
        DECLARE @BronzeColCount INT, @SilverColCount INT;
        DECLARE @MissingCols NVARCHAR(MAX);
        DECLARE @MissingCount INT;
        DECLARE @TestResult VARCHAR(10);
        DECLARE @TestDetails NVARCHAR(MAX);

        SELECT @BronzeColCount = COUNT(*)
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ''bronze_salesforce''
          AND TABLE_NAME = @T
          AND COLUMN_NAME NOT IN (''PCBronzeBatchId'', ''PCVersionDatetime'', ''PCDataHash'');

        SELECT @SilverColCount = COUNT(*)
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ''silver_salesforce''
          AND TABLE_NAME = @T
          AND COLUMN_NAME NOT IN (''PCBronzeBatchId'', ''PCVersionDatetime'', ''PCDataHash'');

        SELECT 
            @MissingCount = COUNT(*),
            @MissingCols = STRING_AGG(CAST(b.COLUMN_NAME AS NVARCHAR(MAX)), '', '') WITHIN GROUP (ORDER BY b.ORDINAL_POSITION)
        FROM INFORMATION_SCHEMA.COLUMNS b
        LEFT JOIN INFORMATION_SCHEMA.COLUMNS s
            ON b.COLUMN_NAME = s.COLUMN_NAME
           AND s.TABLE_SCHEMA = ''silver_salesforce''
           AND s.TABLE_NAME = @T
        WHERE b.TABLE_SCHEMA = ''bronze_salesforce''
          AND b.TABLE_NAME = @T
          AND s.COLUMN_NAME IS NULL
          AND b.COLUMN_NAME NOT IN (''PCBronzeBatchId'', ''PCVersionDatetime'', ''PCDataHash'');

        IF @BronzeColCount = @SilverColCount AND ISNULL(@MissingCount, 0) = 0
        BEGIN
            SET @TestResult = ''PASS'';
            SET @TestDetails = ''Column counts match: '' + CAST(@BronzeColCount AS VARCHAR) + '' columns. All Bronze table columns exist in Silver.'';
        END
        ELSE
        BEGIN
            SET @TestResult = ''FAIL'';
            SET @TestDetails = ''Bronze columns: '' + CAST(@BronzeColCount AS VARCHAR) + 
                               '', Silver columns: '' + CAST(@SilverColCount AS VARCHAR) + 
                               '', Difference: '' + CAST(ABS(@BronzeColCount - @SilverColCount) AS VARCHAR);
            
            IF ISNULL(@MissingCount, 0) > 0
            BEGIN
                SET @TestDetails = @TestDetails + ''. Missing '' + CAST(@MissingCount AS VARCHAR) + 
                                   '' columns in Silver: '' + ISNULL(@MissingCols, ''None'');
            END
        END

        INSERT INTO #TableValidationResults (TableName, TestNumber, TestName, Result, Details)
        VALUES (@T, 1, ''Column Validation'', @TestResult, @TestDetails);
        ';
        EXEC sp_executesql @SQL, N'@T SYSNAME', @T = @TableName;
    END TRY
    BEGIN CATCH
        INSERT INTO #TableValidationResults (TableName, TestNumber, TestName, Result, Details)
        VALUES (@TableName, 1, 'Column Validation', 'ERROR', ERROR_MESSAGE());
    END CATCH

    FETCH NEXT FROM TableCursor1 INTO @TableName;
END

CLOSE TableCursor1;
DEALLOCATE TableCursor1;
PRINT 'Test 1 Complete.';
PRINT '';

-- ============================================================================
-- TEST 2: DATA TYPE COMPARISON
-- ============================================================================
PRINT '============================================================================';
PRINT 'TEST 2: DATA TYPE COMPARISON';
PRINT '============================================================================';

DECLARE TableCursor2 CURSOR FAST_FORWARD FOR
SELECT TableName FROM @TablesToValidate ORDER BY TableName;

OPEN TableCursor2;
FETCH NEXT FROM TableCursor2 INTO @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT 'Processing: ' + @TableName;

    BEGIN TRY
        SET @SQL = N'
        DECLARE @MismatchCount INT;
        DECLARE @MismatchDetails NVARCHAR(MAX);

        SELECT @MismatchCount = COUNT(*)
        FROM INFORMATION_SCHEMA.COLUMNS b
        INNER JOIN INFORMATION_SCHEMA.COLUMNS s
            ON b.COLUMN_NAME = s.COLUMN_NAME
           AND s.TABLE_SCHEMA = ''silver_salesforce''
           AND s.TABLE_NAME = @T
        WHERE b.TABLE_SCHEMA = ''bronze_salesforce''
          AND b.TABLE_NAME = @T
          AND (
              b.DATA_TYPE <> s.DATA_TYPE
              OR ISNULL(CAST(b.CHARACTER_MAXIMUM_LENGTH AS INT), -1) <> ISNULL(CAST(s.CHARACTER_MAXIMUM_LENGTH AS INT), -1)
              OR ISNULL(CAST(b.NUMERIC_PRECISION AS INT), -1) <> ISNULL(CAST(s.NUMERIC_PRECISION AS INT), -1)
              OR ISNULL(CAST(b.NUMERIC_SCALE AS INT), -1) <> ISNULL(CAST(s.NUMERIC_SCALE AS INT), -1)
          )
          AND b.COLUMN_NAME NOT IN (''PCBronzeBatchId'', ''PCVersionDatetime'', ''PCDataHash'');

        SELECT @MismatchDetails = STRING_AGG(
            CAST(
                CONCAT(
                    b.COLUMN_NAME, 
                    '' (Bronze: '', 
                    b.DATA_TYPE,
                    CASE WHEN b.CHARACTER_MAXIMUM_LENGTH IS NOT NULL 
                         THEN ''('' + CAST(b.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + '')'' 
                         ELSE '''' END,
                    '' vs Silver: '', 
                    s.DATA_TYPE,
                    CASE WHEN s.CHARACTER_MAXIMUM_LENGTH IS NOT NULL 
                         THEN ''('' + CAST(s.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + '')'' 
                         ELSE '''' END,
                    '')''
                ) AS NVARCHAR(MAX)
            ), 
            ''; ''
        ) WITHIN GROUP (ORDER BY b.ORDINAL_POSITION)
        FROM INFORMATION_SCHEMA.COLUMNS b
        INNER JOIN INFORMATION_SCHEMA.COLUMNS s
            ON b.COLUMN_NAME = s.COLUMN_NAME
           AND s.TABLE_SCHEMA = ''silver_salesforce''
           AND s.TABLE_NAME = @T
        WHERE b.TABLE_SCHEMA = ''bronze_salesforce''
          AND b.TABLE_NAME = @T
          AND (
              b.DATA_TYPE <> s.DATA_TYPE
              OR ISNULL(CAST(b.CHARACTER_MAXIMUM_LENGTH AS INT), -1) <> ISNULL(CAST(s.CHARACTER_MAXIMUM_LENGTH AS INT), -1)
              OR ISNULL(CAST(b.NUMERIC_PRECISION AS INT), -1) <> ISNULL(CAST(s.NUMERIC_PRECISION AS INT), -1)
              OR ISNULL(CAST(b.NUMERIC_SCALE AS INT), -1) <> ISNULL(CAST(s.NUMERIC_SCALE AS INT), -1)
          )
          AND b.COLUMN_NAME NOT IN (''PCBronzeBatchId'', ''PCVersionDatetime'', ''PCDataHash'');

        INSERT INTO #TableValidationResults (TableName, TestNumber, TestName, Result, Details)
        VALUES (
            @T,
            2,
            ''Data Type Comparison'',
            CASE WHEN ISNULL(@MismatchCount, 0) = 0 THEN ''PASS'' ELSE ''FAIL'' END,
            CASE 
                WHEN ISNULL(@MismatchCount, 0) = 0 THEN ''All data types match between Bronze and Silver''
                ELSE ''Found '' + CAST(@MismatchCount AS VARCHAR) + '' mismatches: '' + ISNULL(@MismatchDetails, ''None'')
            END
        );
        ';
        EXEC sp_executesql @SQL, N'@T SYSNAME', @T = @TableName;
    END TRY
    BEGIN CATCH
        INSERT INTO #TableValidationResults (TableName, TestNumber, TestName, Result, Details)
        VALUES (@TableName, 2, 'Data Type Comparison', 'ERROR', ERROR_MESSAGE());
    END CATCH

    FETCH NEXT FROM TableCursor2 INTO @TableName;
END

CLOSE TableCursor2;
DEALLOCATE TableCursor2;
PRINT 'Test 2 Complete.';
PRINT '';

/*============================================================================
-- TEST 3: DUPLICATE CHECK
--   Bronze: Composite Key = Id + SystemModstamp   (informational only)
--   Silver: Composite Key = Id + SystemModstamp   (enforced)
============================================================================*/

PRINT '============================================================================';
PRINT 'TEST 3: DUPLICATE CHECK (PASS if Silver has no duplicates; Bronze duplicates ignored)';
PRINT '============================================================================';

DECLARE TableCursor3 CURSOR FAST_FORWARD FOR
SELECT TableName FROM @TablesToValidate ORDER BY TableName;

OPEN TableCursor3;
FETCH NEXT FROM TableCursor3 INTO @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT 'Processing: ' + @TableName;

    BEGIN TRY
        SET @SQL = N'
        DECLARE @BronzeDups INT = 0, @SilverDups INT = 0;

        -- Bronze duplicate composite keys (Id|SystemModstamp) - informational only
        SELECT @BronzeDups = COUNT(*)
        FROM (
            SELECT 1 AS DupGroup
            FROM [bronze_salesforce].' + QUOTENAME(@TableName) + N'
            GROUP BY
                CAST([Id] AS NVARCHAR(100)),
                CONVERT(NVARCHAR(30), [SystemModstamp], 126)
            HAVING COUNT(*) > 1
        ) AS b;

        -- Silver duplicate composite keys (Id|SystemModstamp) - enforced
        SELECT @SilverDups = COUNT(*)
        FROM (
            SELECT 1 AS DupGroup
            FROM [silver_salesforce].' + QUOTENAME(@TableName) + N'
            GROUP BY
                CAST([Id] AS NVARCHAR(100)),
                CONVERT(NVARCHAR(30), [SystemModstamp], 126)
            HAVING COUNT(*) > 1
        ) AS s;

        INSERT INTO #TableValidationResults (TableName, TestNumber, TestName, Result, Details)
        VALUES (
            @T,
            3,
            ''Duplicate Check (Id|SystemModstamp)'',
            CASE WHEN ISNULL(@SilverDups, 0) = 0 THEN ''PASS'' ELSE ''FAIL'' END,
            CASE
                WHEN ISNULL(@SilverDups, 0) = 0
                    THEN ''PASS: Silver has no duplicates on (Id|SystemModstamp). Bronze duplicate groups (Id|SystemModstamp): '' + CAST(ISNULL(@BronzeDups,0) AS VARCHAR(20))
                ELSE
                    ''FAIL: Silver duplicate groups on (Id|SystemModstamp): '' + CAST(ISNULL(@SilverDups,0) AS VARCHAR(20)) +
                    ''. Bronze duplicate groups (Id|SystemModstamp): '' + CAST(ISNULL(@BronzeDups,0) AS VARCHAR(20))
            END
        );
        ';
        EXEC sp_executesql @SQL, N'@T SYSNAME', @T = @TableName;
    END TRY
    BEGIN CATCH
        INSERT INTO #TableValidationResults (TableName, TestNumber, TestName, Result, Details)
        VALUES (@TableName, 3, 'Duplicate Check (Id|SystemModstamp)', 'ERROR', ERROR_MESSAGE());
    END CATCH

    FETCH NEXT FROM TableCursor3 INTO @TableName;
END

CLOSE TableCursor3;
DEALLOCATE TableCursor3;

PRINT 'Test 3 Complete.';
PRINT '';

-- ============================================================================
-- TEST 4: ROW COUNT COMPARISON
-- ============================================================================
PRINT '============================================================================';
PRINT 'TEST 4: ROW COUNT COMPARISON';
PRINT '============================================================================';

DECLARE TableCursor4 CURSOR FAST_FORWARD FOR
SELECT TableName FROM @TablesToValidate ORDER BY TableName;

OPEN TableCursor4;
FETCH NEXT FROM TableCursor4 INTO @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT 'Processing: ' + @TableName;

    BEGIN TRY
        SET @SQL = N'
        DECLARE @BronzeCount BIGINT, @SilverCount BIGINT, @SimilarityPct DECIMAL(10,2);

        SELECT @BronzeCount = COUNT(*) FROM bronze_salesforce.' + QUOTENAME(@TableName) + N';
        SELECT @SilverCount = COUNT(*) FROM silver_salesforce.' + QUOTENAME(@TableName) + N';

        SET @SimilarityPct = CASE 
            WHEN @BronzeCount = 0 THEN 0
            ELSE (CAST(@SilverCount AS DECIMAL(18,2)) / @BronzeCount) * 100
        END;

        INSERT INTO #TableValidationResults (TableName, TestNumber, TestName, Result, Details)
        VALUES (
            @T,
            4,
            ''Row Count Comparison'',
            CASE 
                WHEN @BronzeCount = @SilverCount THEN ''PASS''
                WHEN @SimilarityPct >= 100.0 THEN ''PASS''
                WHEN @SimilarityPct >= 99.0 THEN ''WARNING''
                ELSE ''FAIL''
            END,
            CONCAT(
                ''Bronze: '', @BronzeCount,
                '', Silver: '', @SilverCount,
                '', Difference: '', ABS(@BronzeCount - @SilverCount),
                '', Similarity: '', CAST(@SimilarityPct AS VARCHAR), ''%''
            )
        );
        ';
        EXEC sp_executesql @SQL, N'@T SYSNAME', @T = @TableName;
    END TRY
    BEGIN CATCH
        INSERT INTO #TableValidationResults (TableName, TestNumber, TestName, Result, Details)
        VALUES (@TableName, 4, 'Row Count Comparison', 'ERROR', ERROR_MESSAGE());
    END CATCH

    FETCH NEXT FROM TableCursor4 INTO @TableName;
END

CLOSE TableCursor4;
DEALLOCATE TableCursor4;
PRINT 'Test 4 Complete.';
PRINT '';
-- ============================================================================
-- TEST 5: ROW EXISTENCE CHECK (Composite Key = Id + SystemModstamp)
-- ============================================================================
PRINT '============================================================================';
PRINT 'TEST 5: ROW EXISTENCE CHECK (LOG MISSING COMPOSITE KEYS)';
PRINT '============================================================================';

DECLARE @TopIdsToShow INT = 50;  -- change this number as needed

DECLARE TableCursor5 CURSOR FAST_FORWARD FOR
SELECT TableName FROM @TablesToValidate ORDER BY TableName;

OPEN TableCursor5;
FETCH NEXT FROM TableCursor5 INTO @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT 'Processing: ' + @TableName;

    BEGIN TRY
        SET @SQL = N'
        DECLARE @OnlyInBronze INT = 0,
                @OnlyInSilver INT = 0,
                @MissingCount INT = 0;

        DECLARE @OnlyInBronzeIds NVARCHAR(MAX) = NULL,
                @OnlyInSilverIds NVARCHAR(MAX) = NULL;

        -- Count: composite keys in Bronze but not in Silver
        SELECT @OnlyInBronze = COUNT(*)
        FROM bronze_salesforce.' + QUOTENAME(@TableName) + N' b
        LEFT JOIN silver_salesforce.' + QUOTENAME(@TableName) + N' s
            ON CONCAT(CAST(b.Id AS NVARCHAR(100)), ''|'', CONVERT(NVARCHAR(30), b.SystemModstamp, 126))
             = CONCAT(CAST(s.Id AS NVARCHAR(100)), ''|'', CONVERT(NVARCHAR(30), s.SystemModstamp, 126))
        WHERE s.Id IS NULL;

        -- Count: composite keys in Silver but not in Bronze
        SELECT @OnlyInSilver = COUNT(*)
        FROM silver_salesforce.' + QUOTENAME(@TableName) + N' s
        LEFT JOIN bronze_salesforce.' + QUOTENAME(@TableName) + N' b
            ON CONCAT(CAST(b.Id AS NVARCHAR(100)), ''|'', CONVERT(NVARCHAR(30), b.SystemModstamp, 126))
             = CONCAT(CAST(s.Id AS NVARCHAR(100)), ''|'', CONVERT(NVARCHAR(30), s.SystemModstamp, 126))
        WHERE b.Id IS NULL;

        SET @MissingCount = ISNULL(@OnlyInBronze,0) + ISNULL(@OnlyInSilver,0);

        -- TOP N: Bronze-only composite keys
        SELECT @OnlyInBronzeIds = STRING_AGG(CAST(x.CompositeKey AS NVARCHAR(200)), '','')
        FROM (
            SELECT TOP (@TopN)
                CONCAT(CAST(b.Id AS NVARCHAR(100)), ''|'', CONVERT(NVARCHAR(30), b.SystemModstamp, 126)) AS CompositeKey
            FROM bronze_salesforce.' + QUOTENAME(@TableName) + N' b
            LEFT JOIN silver_salesforce.' + QUOTENAME(@TableName) + N' s
                ON CONCAT(CAST(b.Id AS NVARCHAR(100)), ''|'', CONVERT(NVARCHAR(30), b.SystemModstamp, 126))
                 = CONCAT(CAST(s.Id AS NVARCHAR(100)), ''|'', CONVERT(NVARCHAR(30), s.SystemModstamp, 126))
            WHERE s.Id IS NULL
            ORDER BY b.Id
        ) x;

        -- TOP N: Silver-only composite keys
        SELECT @OnlyInSilverIds = STRING_AGG(CAST(x.CompositeKey AS NVARCHAR(200)), '','')
        FROM (
            SELECT TOP (@TopN)
                CONCAT(CAST(s.Id AS NVARCHAR(100)), ''|'', CONVERT(NVARCHAR(30), s.SystemModstamp, 126)) AS CompositeKey
            FROM silver_salesforce.' + QUOTENAME(@TableName) + N' s
            LEFT JOIN bronze_salesforce.' + QUOTENAME(@TableName) + N' b
                ON CONCAT(CAST(b.Id AS NVARCHAR(100)), ''|'', CONVERT(NVARCHAR(30), b.SystemModstamp, 126))
                 = CONCAT(CAST(s.Id AS NVARCHAR(100)), ''|'', CONVERT(NVARCHAR(30), s.SystemModstamp, 126))
            WHERE b.Id IS NULL
            ORDER BY s.Id
        ) x;

        INSERT INTO #TableValidationResults
        (
            TableName, TestNumber, TestName, Result, Details,
            OnlyInBronzeCount, OnlyInSilverCount, BronzeOnlyIds, SilverOnlyIds
        )
        VALUES
        (
            @T,
            5,
            ''Row Existence Check (Id + SystemModstamp)'',
            CASE WHEN @MissingCount = 0 THEN ''PASS'' ELSE ''FAIL'' END,
            CASE 
                WHEN @MissingCount = 0 THEN ''All composite keys (Id|SystemModstamp) exist in both tables''
                ELSE ''Missing composite keys detected (see BronzeOnlyIds / SilverOnlyIds columns)''
            END,
            @OnlyInBronze,
            @OnlyInSilver,
            ISNULL(@OnlyInBronzeIds, ''None''),
            ISNULL(@OnlyInSilverIds, ''None'')
        );
        ';

        EXEC sp_executesql
            @SQL,
            N'@T SYSNAME, @TopN INT',
            @T = @TableName,
            @TopN = @TopIdsToShow;
    END TRY
    BEGIN CATCH
        INSERT INTO #TableValidationResults (TableName, TestNumber, TestName, Result, Details)
        VALUES (@TableName, 5, 'Row Existence Check (Id + SystemModstamp)', 'ERROR', ERROR_MESSAGE());
    END CATCH

    FETCH NEXT FROM TableCursor5 INTO @TableName;
END

CLOSE TableCursor5;
DEALLOCATE TableCursor5;

PRINT 'Test 5 Complete.';
PRINT '';
-- ============================================================================
-- FINAL RESULTS
-- ============================================================================
PRINT '============================================================================';
PRINT 'VALIDATION RESULTS';
PRINT '============================================================================';
PRINT '';

SELECT 
    TestNumber,
    TestName,
    TableName,
    Result,
    Details,
    OnlyInBronzeCount,
    OnlyInSilverCount,
    BronzeOnlyIds,
    SilverOnlyIds,
    CONVERT(VARCHAR(30), ExecutionTime, 120) AS ExecutionTime
FROM #TableValidationResults
ORDER BY 
    TestNumber,
    TestName,
    TableName,
    ExecutionTime;

-- ============================================================================
-- SUMMARY STATISTICS
-- ============================================================================
PRINT '';
PRINT '============================================================================';
PRINT 'SUMMARY STATISTICS';
PRINT '============================================================================';

DECLARE @TotalTests INT;
DECLARE @PassedTests INT;
DECLARE @FailedTests INT;
DECLARE @WarningTests INT;
DECLARE @SkippedTests INT;
DECLARE @ErrorTests INT;

SELECT 
    @TotalTests = COUNT(*),
    @PassedTests = SUM(CASE WHEN Result = 'PASS' THEN 1 ELSE 0 END),
    @FailedTests = SUM(CASE WHEN Result = 'FAIL' THEN 1 ELSE 0 END),
    @WarningTests = SUM(CASE WHEN Result = 'WARNING' THEN 1 ELSE 0 END),
    @SkippedTests = SUM(CASE WHEN Result = 'SKIP' THEN 1 ELSE 0 END),
    @ErrorTests = SUM(CASE WHEN Result = 'ERROR' THEN 1 ELSE 0 END)
FROM #TableValidationResults;

PRINT 'Total Tables Processed: ' + CAST(@TotalTables AS VARCHAR);
PRINT 'Total Tests Executed: ' + CAST(@TotalTests AS VARCHAR) + ' (5 tests per table)';
PRINT 'Passed: ' + CAST(@PassedTests AS VARCHAR);
PRINT 'Failed: ' + CAST(@FailedTests AS VARCHAR);
PRINT 'Warnings: ' + CAST(@WarningTests AS VARCHAR);
PRINT 'Skipped: ' + CAST(@SkippedTests AS VARCHAR);
PRINT 'Errors: ' + CAST(@ErrorTests AS VARCHAR);
PRINT '';

-- Per-table summary
PRINT '--- PER-TABLE SUMMARY ---';
SELECT 
    TableName,
    COUNT(*) AS TotalTests,
    SUM(CASE WHEN Result = 'PASS' THEN 1 ELSE 0 END) AS Passed,
    SUM(CASE WHEN Result = 'FAIL' THEN 1 ELSE 0 END) AS Failed,
    SUM(CASE WHEN Result = 'WARNING' THEN 1 ELSE 0 END) AS Warnings,
    SUM(CASE WHEN Result = 'SKIP' THEN 1 ELSE 0 END) AS Skipped,
    SUM(CASE WHEN Result = 'ERROR' THEN 1 ELSE 0 END) AS Errors,
    CASE 
        WHEN SUM(CASE WHEN Result = 'FAIL' THEN 1 ELSE 0 END) > 0 THEN 'FAILED'
        WHEN SUM(CASE WHEN Result = 'ERROR' THEN 1 ELSE 0 END) > 0 THEN 'ERROR'
        WHEN SUM(CASE WHEN Result = 'WARNING' THEN 1 ELSE 0 END) > 0 THEN 'PASSED WITH WARNINGS'
        ELSE 'PASSED'
    END AS OverallStatus
FROM #TableValidationResults
GROUP BY TableName
ORDER BY TableName;

PRINT '';
PRINT 'Overall Validation Status: ' + 
    CASE 
        WHEN @FailedTests = 0 AND @ErrorTests = 0 AND @WarningTests = 0 THEN 'ALL TESTS PASSED'
        WHEN @FailedTests = 0 AND @ErrorTests = 0 THEN 'PASSED WITH WARNINGS'
        WHEN @ErrorTests > 0 THEN 'COMPLETED WITH ERRORS'
        ELSE 'VALIDATION FAILED'
    END;

PRINT '============================================================================';
PRINT 'Validation Complete: ' + CONVERT(VARCHAR(30), GETDATE(), 120);
PRINT '============================================================================';

-- Clean up
DROP TABLE #TableValidationResults;

SET NOCOUNT OFF;
