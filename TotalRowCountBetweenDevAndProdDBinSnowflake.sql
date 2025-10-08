USE WAREHOUSE DBT_DEV_WH;

SELECT 
    'RAW_SALESFORCE' AS schema_name,
    'Account' AS table_name,
    p.prod_count,
    t.test_count,
    p.prod_count - t.test_count AS difference
FROM (SELECT COUNT(*) AS prod_count FROM DW_V2.RAW_SALESFORCE.Account) p
CROSS JOIN (SELECT COUNT(*) AS test_count FROM DW_V2_TEST.RAW_SALESFORCE.Account) t
UNION ALL
SELECT 
    'RAW_SALESFORCE' AS schema_name,
    'AccountContactRelation' AS table_name,
    p.prod_count,
    t.test_count,
    p.prod_count - t.test_count AS difference
FROM (SELECT COUNT(*) AS prod_count FROM DW_V2.RAW_SALESFORCE.AccountContactRelation) p
CROSS JOIN (SELECT COUNT(*) AS test_count FROM DW_V2_TEST.RAW_SALESFORCE.AccountContactRelation) t
UNION ALL
SELECT 
    'RAW_SALESFORCE' AS schema_name,
    'AccountTeamMember' AS table_name,
    p.prod_count,
    t.test_count,
    p.prod_count - t.test_count AS difference
FROM (SELECT COUNT(*) AS prod_count FROM DW_V2.RAW_SALESFORCE.AccountTeamMember) p
CROSS JOIN (SELECT COUNT(*) AS test_count FROM DW_V2_TEST.RAW_SALESFORCE.AccountTeamMember) t
UNION ALL
SELECT 
    'RAW_SALESFORCE' AS schema_name,
    'Contact' AS table_name,
    p.prod_count,
    t.test_count,
    p.prod_count - t.test_count AS difference
FROM (SELECT COUNT(*) AS prod_count FROM DW_V2.RAW_SALESFORCE.Contact) p
CROSS JOIN (SELECT COUNT(*) AS test_count FROM DW_V2_TEST.RAW_SALESFORCE.Contact) t
UNION ALL
SELECT 
    'RAW_SALESFORCE' AS schema_name,
    'Market__c' AS table_name,
    p.prod_count,
    t.test_count,
    p.prod_count - t.test_count AS difference
FROM (SELECT COUNT(*) AS prod_count FROM DW_V2.RAW_SALESFORCE.Market__c) p
CROSS JOIN (SELECT COUNT(*) AS test_count FROM DW_V2_TEST.RAW_SALESFORCE.Market__c) t
UNION ALL
SELECT 
    'RAW_SALESFORCE' AS schema_name,
    'Opportunity' AS table_name,
    p.prod_count,
    t.test_count,
    p.prod_count - t.test_count AS difference
FROM (SELECT COUNT(*) AS prod_count FROM DW_V2.RAW_SALESFORCE.Opportunity) p
CROSS JOIN (SELECT COUNT(*) AS test_count FROM DW_V2_TEST.RAW_SALESFORCE.Opportunity) t
UNION ALL
SELECT 
    'RAW_SALESFORCE' AS schema_name,
    'OpportunityLineItem' AS table_name,
    p.prod_count,
    t.test_count,
    p.prod_count - t.test_count AS difference
FROM (SELECT COUNT(*) AS prod_count FROM DW_V2.RAW_SALESFORCE.OpportunityLineItem) p
CROSS JOIN (SELECT COUNT(*) AS test_count FROM DW_V2_TEST.RAW_SALESFORCE.OpportunityLineItem) t
UNION ALL
SELECT 
    'RAW_SALESFORCE' AS schema_name,
    'Product2' AS table_name,
    p.prod_count,
    t.test_count,
    p.prod_count - t.test_count AS difference
FROM (SELECT COUNT(*) AS prod_count FROM DW_V2.RAW_SALESFORCE.Product2) p
CROSS JOIN (SELECT COUNT(*) AS test_count FROM DW_V2_TEST.RAW_SALESFORCE.Product2) t
UNION ALL
SELECT 
    'RAW_SALESFORCE' AS schema_name,
    'RecordType' AS table_name,
    p.prod_count,
    t.test_count,
    p.prod_count - t.test_count AS difference
FROM (SELECT COUNT(*) AS prod_count FROM DW_V2.RAW_SALESFORCE.RecordType) p
CROSS JOIN (SELECT COUNT(*) AS test_count FROM DW_V2_TEST.RAW_SALESFORCE.RecordType) t
UNION ALL
SELECT 
    'RAW_SALESFORCE' AS schema_name,
    'Region__c' AS table_name,
    p.prod_count,
    t.test_count,
    p.prod_count - t.test_count AS difference
FROM (SELECT COUNT(*) AS prod_count FROM DW_V2.RAW_SALESFORCE.Region__c) p
CROSS JOIN (SELECT COUNT(*) AS test_count FROM DW_V2_TEST.RAW_SALESFORCE.Region__c) t
UNION ALL
SELECT 
    'RAW_SALESFORCE' AS schema_name,
    'Sub_Market__c' AS table_name,
    p.prod_count,
    t.test_count,
    p.prod_count - t.test_count AS difference
FROM (SELECT COUNT(*) AS prod_count FROM DW_V2.RAW_SALESFORCE.Sub_Market__c) p
CROSS JOIN (SELECT COUNT(*) AS test_count FROM DW_V2_TEST.RAW_SALESFORCE.Sub_Market__c) t
UNION ALL
SELECT 
    'RAW_SALESFORCE' AS schema_name,
    'Task' AS table_name,
    p.prod_count,
    t.test_count,
    p.prod_count - t.test_count AS difference
FROM (SELECT COUNT(*) AS prod_count FROM DW_V2.RAW_SALESFORCE.Task) p
CROSS JOIN (SELECT COUNT(*) AS test_count FROM DW_V2_TEST.RAW_SALESFORCE.Task) t
UNION ALL
SELECT 
    'RAW_SALESFORCE' AS schema_name,
    'User' AS table_name,
    p.prod_count,
    t.test_count,
    p.prod_count - t.test_count AS difference
FROM (SELECT COUNT(*) AS prod_count FROM DW_V2.RAW_SALESFORCE.User) p
CROSS JOIN (SELECT COUNT(*) AS test_count FROM DW_V2_TEST.RAW_SALESFORCE.User) t;