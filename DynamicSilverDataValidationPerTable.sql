-- Ensure temp results table exists (create if missing)
IF OBJECT_ID('tempdb..#TableValidationResults') IS NULL
BEGIN
    CREATE TABLE #TableValidationResults (
        TableName          SYSNAME,
        TestNumber         INT,
        TestName           VARCHAR(100),
        Result             VARCHAR(10),
        Details            NVARCHAR(MAX),

        OnlyInBronzeCount  INT NULL,
        OnlyInSilverCount  INT NULL,
        BronzeOnlyIds      NVARCHAR(MAX) NULL,
        SilverOnlyIds      NVARCHAR(MAX) NULL,

        ExecutionTime      DATETIME DEFAULT GETDATE()
    );
END

-- ============================================================================
-- TEST 5: ROW EXISTENCE CHECK (LOG MISSING IDS) - RecordType ONLY
-- ============================================================================
PRINT '============================================================================';
PRINT 'TEST 5: ROW EXISTENCE CHECK (LOG MISSING IDS) - RecordType';
PRINT '============================================================================';

DECLARE @TopIdsToShow INT = 50;  -- change this number as needed
DECLARE @TableName SYSNAME = 'RecordType';

PRINT 'Processing: ' + @TableName;

BEGIN TRY
    DECLARE @OnlyInBronze INT = 0,
            @OnlyInSilver INT = 0,
            @MissingCount INT = 0;

    DECLARE @OnlyInBronzeIds NVARCHAR(MAX) = NULL,
            @OnlyInSilverIds NVARCHAR(MAX) = NULL;

    -- Count: Ids in Bronze but not in Silver
    SELECT @OnlyInBronze = COUNT(*)
    FROM bronze_salesforce.[RecordType] b
    LEFT JOIN silver_salesforce.[RecordType] s
        ON b.Id = s.Id
    WHERE s.Id IS NULL;

    -- Count: Ids in Silver but not in Bronze
    SELECT @OnlyInSilver = COUNT(*)
    FROM silver_salesforce.[RecordType] s
    LEFT JOIN bronze_salesforce.[RecordType] b
        ON s.Id = b.Id
    WHERE b.Id IS NULL;

    SET @MissingCount = ISNULL(@OnlyInBronze,0) + ISNULL(@OnlyInSilver,0);

    -- TOP N list: Bronze-only Ids
    SELECT @OnlyInBronzeIds = STRING_AGG(CAST(x.Id AS NVARCHAR(100)), ',')
    FROM (
        SELECT TOP (@TopIdsToShow) b.Id
        FROM bronze_salesforce.[RecordType] b
        LEFT JOIN silver_salesforce.[RecordType] s
            ON b.Id = s.Id
        WHERE s.Id IS NULL
        ORDER BY b.Id
    ) x;

    -- TOP N list: Silver-only Ids
    SELECT @OnlyInSilverIds = STRING_AGG(CAST(x.Id AS NVARCHAR(100)), ',')
    FROM (
        SELECT TOP (@TopIdsToShow) s.Id
        FROM silver_salesforce.[RecordType] s
        LEFT JOIN bronze_salesforce.[RecordType] b
            ON s.Id = b.Id
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
        @TableName,
        5,
        'Row Existence Check',
        CASE WHEN @MissingCount = 0 THEN 'PASS' ELSE 'FAIL' END,
        CASE 
            WHEN @MissingCount = 0 THEN 'All Ids exist in both tables'
            ELSE 'Missing Ids detected (see BronzeOnlyIds / SilverOnlyIds columns)'
        END,
        @OnlyInBronze,
        @OnlyInSilver,
        ISNULL(@OnlyInBronzeIds, 'None'),
        ISNULL(@OnlyInSilverIds, 'None')
    );
END TRY
BEGIN CATCH
    INSERT INTO #TableValidationResults (TableName, TestNumber, TestName, Result, Details)
    VALUES ('RecordType', 5, 'Row Existence Check', 'ERROR', ERROR_MESSAGE());
END CATCH;

-- Show the result row(s) for this test
SELECT *
FROM #TableValidationResults
WHERE TableName = 'RecordType' AND TestNumber = 5
ORDER BY ExecutionTime DESC;


