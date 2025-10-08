- Check RAW_SALESFORCE.Account
WITH hashes AS (
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'Account' AS table_name,
           'DW_V2' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2.RAW_SALESFORCE.Account
    UNION ALL
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'Account' AS table_name,
           'DW_V2_TEST' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2_TEST.RAW_SALESFORCE.Account
)
SELECT 
    MAX(schema_name) AS schema_name,
    MAX(table_name) AS table_name,
    MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) AS prod_hash,
    MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END) AS test_hash,
    CASE 
        WHEN MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) =
             MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END)
        THEN 'MATCH' 
        ELSE 'MISMATCH' 
    END AS status
FROM hashes;

-- Check RAW_SALESFORCE.AccountContactRelation
WITH hashes AS (
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'AccountContactRelation' AS table_name,
           'DW_V2' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2.RAW_SALESFORCE.AccountContactRelation
    UNION ALL
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'AccountContactRelation' AS table_name,
           'DW_V2_TEST' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2_TEST.RAW_SALESFORCE.AccountContactRelation
)
SELECT 
    MAX(schema_name) AS schema_name,
    MAX(table_name) AS table_name,
    MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) AS prod_hash,
    MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END) AS test_hash,
    CASE 
        WHEN MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) =
             MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END)
        THEN 'MATCH' 
        ELSE 'MISMATCH' 
    END AS status
FROM hashes;

-- Check RAW_SALESFORCE.AccountTeamMember
WITH hashes AS (
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'AccountTeamMember' AS table_name,
           'DW_V2' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2.RAW_SALESFORCE.AccountTeamMember
    UNION ALL
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'AccountTeamMember' AS table_name,
           'DW_V2_TEST' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2_TEST.RAW_SALESFORCE.AccountTeamMember
)
SELECT 
    MAX(schema_name) AS schema_name,
    MAX(table_name) AS table_name,
    MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) AS prod_hash,
    MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END) AS test_hash,
    CASE 
        WHEN MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) =
             MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END)
        THEN 'MATCH' 
        ELSE 'MISMATCH' 
    END AS status
FROM hashes;

-- Check RAW_SALESFORCE.Contact
WITH hashes AS (
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'Contact' AS table_name,
           'DW_V2' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2.RAW_SALESFORCE.Contact
    UNION ALL
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'Contact' AS table_name,
           'DW_V2_TEST' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2_TEST.RAW_SALESFORCE.Contact
)
SELECT 
    MAX(schema_name) AS schema_name,
    MAX(table_name) AS table_name,
    MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) AS prod_hash,
    MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END) AS test_hash,
    CASE 
        WHEN MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) =
             MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END)
        THEN 'MATCH' 
        ELSE 'MISMATCH' 
    END AS status
FROM hashes;

-- Check RAW_SALESFORCE.Market__c
WITH hashes AS (
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'Market__c' AS table_name,
           'DW_V2' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2.RAW_SALESFORCE.Market__c
    UNION ALL
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'Market__c' AS table_name,
           'DW_V2_TEST' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2_TEST.RAW_SALESFORCE.Market__c
)
SELECT 
    MAX(schema_name) AS schema_name,
    MAX(table_name) AS table_name,
    MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) AS prod_hash,
    MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END) AS test_hash,
    CASE 
        WHEN MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) =
             MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END)
        THEN 'MATCH' 
        ELSE 'MISMATCH' 
    END AS status
FROM hashes;

-- Check RAW_SALESFORCE.Opportunity
WITH hashes AS (
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'Opportunity' AS table_name,
           'DW_V2' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2.RAW_SALESFORCE.Opportunity
    UNION ALL
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'Opportunity' AS table_name,
           'DW_V2_TEST' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2_TEST.RAW_SALESFORCE.Opportunity
)
SELECT 
    MAX(schema_name) AS schema_name,
    MAX(table_name) AS table_name,
    MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) AS prod_hash,
    MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END) AS test_hash,
    CASE 
        WHEN MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) =
             MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END)
        THEN 'MATCH' 
        ELSE 'MISMATCH' 
    END AS status
FROM hashes;

-- Check RAW_SALESFORCE.OpportunityLineItem
WITH hashes AS (
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'OpportunityLineItem' AS table_name,
           'DW_V2' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2.RAW_SALESFORCE.OpportunityLineItem
    UNION ALL
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'OpportunityLineItem' AS table_name,
           'DW_V2_TEST' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2_TEST.RAW_SALESFORCE.OpportunityLineItem
)
SELECT 
    MAX(schema_name) AS schema_name,
    MAX(table_name) AS table_name,
    MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) AS prod_hash,
    MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END) AS test_hash,
    CASE 
        WHEN MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) =
             MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END)
        THEN 'MATCH' 
        ELSE 'MISMATCH' 
    END AS status
FROM hashes;

-- Check RAW_SALESFORCE.Product2
WITH hashes AS (
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'Product2' AS table_name,
           'DW_V2' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2.RAW_SALESFORCE.Product2
    UNION ALL
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'Product2' AS table_name,
           'DW_V2_TEST' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2_TEST.RAW_SALESFORCE.Product2
)
SELECT 
    MAX(schema_name) AS schema_name,
    MAX(table_name) AS table_name,
    MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) AS prod_hash,
    MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END) AS test_hash,
    CASE 
        WHEN MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) =
             MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END)
        THEN 'MATCH' 
        ELSE 'MISMATCH' 
    END AS status
FROM hashes;

-- Check RAW_SALESFORCE.RecordType
WITH hashes AS (
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'RecordType' AS table_name,
           'DW_V2' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2.RAW_SALESFORCE.RecordType
    UNION ALL
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'RecordType' AS table_name,
           'DW_V2_TEST' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2_TEST.RAW_SALESFORCE.RecordType
)
SELECT 
    MAX(schema_name) AS schema_name,
    MAX(table_name) AS table_name,
    MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) AS prod_hash,
    MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END) AS test_hash,
    CASE 
        WHEN MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) =
             MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END)
        THEN 'MATCH' 
        ELSE 'MISMATCH' 
    END AS status
FROM hashes;

-- Check RAW_SALESFORCE.Region__c
WITH hashes AS (
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'Region__c' AS table_name,
           'DW_V2' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2.RAW_SALESFORCE.Region__c
    UNION ALL
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'Region__c' AS table_name,
           'DW_V2_TEST' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2_TEST.RAW_SALESFORCE.Region__c
)
SELECT 
    MAX(schema_name) AS schema_name,
    MAX(table_name) AS table_name,
    MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) AS prod_hash,
    MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END) AS test_hash,
    CASE 
        WHEN MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) =
             MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END)
        THEN 'MATCH' 
        ELSE 'MISMATCH' 
    END AS status
FROM hashes;

-- Check RAW_SALESFORCE.Sub_Market__c
WITH hashes AS (
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'Sub_Market__c' AS table_name,
           'DW_V2' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2.RAW_SALESFORCE.Sub_Market__c
    UNION ALL
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'Sub_Market__c' AS table_name,
           'DW_V2_TEST' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2_TEST.RAW_SALESFORCE.Sub_Market__c
)
SELECT 
    MAX(schema_name) AS schema_name,
    MAX(table_name) AS table_name,
    MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) AS prod_hash,
    MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END) AS test_hash,
    CASE 
        WHEN MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) =
             MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END)
        THEN 'MATCH' 
        ELSE 'MISMATCH' 
    END AS status
FROM hashes;

-- Check RAW_SALESFORCE.Task
WITH hashes AS (
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'Task' AS table_name,
           'DW_V2' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2.RAW_SALESFORCE.Task
    UNION ALL
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'Task' AS table_name,
           'DW_V2_TEST' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2_TEST.RAW_SALESFORCE.Task
)
SELECT 
    MAX(schema_name) AS schema_name,
    MAX(table_name) AS table_name,
    MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) AS prod_hash,
    MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END) AS test_hash,
    CASE 
        WHEN MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) =
             MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END)
        THEN 'MATCH' 
        ELSE 'MISMATCH' 
    END AS status
FROM hashes;

-- Check RAW_SALESFORCE.User
WITH hashes AS (
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'User' AS table_name,
           'DW_V2' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2.RAW_SALESFORCE.User
    UNION ALL
    SELECT 'RAW_SALESFORCE' AS schema_name,
           'User' AS table_name,
           'DW_V2_TEST' AS catalog,
           HASH_AGG(SHA2(TO_VARCHAR(OBJECT_CONSTRUCT(*)), 256)) AS table_hash
    FROM DW_V2_TEST.RAW_SALESFORCE.User
)
SELECT 
    MAX(schema_name) AS schema_name,
    MAX(table_name) AS table_name,
    MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) AS prod_hash,
    MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END) AS test_hash,
    CASE 
        WHEN MAX(CASE WHEN catalog = 'DW_V2' THEN table_hash END) =
             MAX(CASE WHEN catalog = 'DW_V2_TEST' THEN table_hash END)
        THEN 'MATCH' 
        ELSE 'MISMATCH' 
    END AS status
FROM hashes;