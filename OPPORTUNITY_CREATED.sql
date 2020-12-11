
CREATE OR REPLACE VIEW TF_DATA_WAREHOUSE.SALES_AND_MARKETING.OPPORTUNITY_CREATED
AS
WITH signed_history AS (
  SELECT * FROM (SELECT
    AH."ACCOUNT_ID",
    AH."FIELD",
    AH."OLD_VALUE",
    AH."NEW_VALUE",
    AH."CREATED_DATE",
    ROW_NUMBER()OVER(PARTITION BY AH."ACCOUNT_ID" ORDER BY AH."CREATED_DATE" ASC) as "Rank"
    FROM LZ_FIVETRAN.SALESFORCE."ACCOUNT_HISTORY" AH
    WHERE AH."FIELD" = 'Partner_Status__c'
    AND AH."NEW_VALUE" = 'Signed Partner'
    ORDER BY "CREATED_DATE" DESC)z
    WHERE z."Rank" = '1'),
/*Created Date Correction for Nexcess Uploaded Opportunities*/
  CREATED_DATE_FIX AS (
    SELECT
    "ID" AS "OpportunityId",
    CASE WHEN "ORDER_ACCEPTED_DATE_C" IS NOT NULL THEN "ORDER_ACCEPTED_DATE_C"::DATE
    ELSE O."CREATED_DATE"::DATE
        END AS "CREATED_DATE"
    FROM LZ_FIVETRAN.SALESFORCE.OPPORTUNITY O
      )
SELECT
/*Opportunity Objects*/
O."ID" AS "OpportunityId",
O."ACCOUNT_ID",
A."LW_BILLING_ACCOUNT_NUMBER_C",
O."OWNER_ID",
O."NAME",
date_trunc('month', CDF."CREATED_DATE")::date AS "CreatedStartOfMonth",
/*Date that will display in SF*/
O."CREATED_DATE"::DATE AS "SF_CREATED_DATE",
/*Date that is corrected which will show in BI reporting*/
CDF."CREATED_DATE"::DATE AS "CREATED_DATE",
O."CLOSE_DATE",
O."NET_AMOUNT_C" AS "NetAmount",
O."EXPECTED_REVENUE",
CASE WHEN "COMMA_DEAL_C" LIKE 'TRUE' THEN 1 ELSE 0 END AS "COMMA_DEAL_COUNT",
CASE WHEN "COMMA_DEAL_C" LIKE 'TRUE' THEN "NET_AMOUNT_C" ELSE 0 END AS "COMMA_DEAL_AMOUNT",
1 AS "OpportunitiesCreated",
/*new or existing customers Count*/
CASE WHEN O."TYPE" IN ('New Customer','New Customer - Nexcess') THEN 1
  END AS "OpportunitiesNew",
CASE WHEN O."TYPE" IN ('Customer Software EOL Upgrade',
                    'Existing Customer - Reseller',
                    'Existing Customer Upgrade',
                    'Existing Customer Upgrade - Nexcess',
                    'Customer Dedicated Server Upgrade',
                    'Existing Component Upgrade or Resize',
                    'Existing Component Upgrade or Resize - Nexcess',
                    'Returning Customer',
                    'Customer Storm Resize',
                    'Existing Customer New Infrastructure',
                    'Existing Customer New Infrastructure - Nexcess')
                    THEN 1
  END AS "OpportunitiesExisting",
/*new or existing customers Slicer*/
CASE WHEN O."TYPE" IN ('New Customer','New Customer - Nexcess') THEN 'New'
  WHEN O."TYPE" IN ('Customer Software EOL Upgrade',
                    'Existing Customer - Reseller',
                    'Existing Customer Upgrade',
                    'Existing Customer Upgrade - Nexcess',
                    'Customer Dedicated Server Upgrade',
                    'Existing Component Upgrade or Resize',
                    'Existing Component Upgrade or Resize - Nexcess',
                    'Returning Customer',
                    'Customer Storm Resize',
                    'Existing Customer New Infrastructure',
                    'Existing Customer New Infrastructure - Nexcess')
                    THEN 'Existing'
  WHEN O."TYPE" IS NULL THEN 'Unknown'
  ELSE O."TYPE" END AS "IsNewOrExisting",
O."TYPE",
O."STAGE_NAME",
O."TYPE" AS "OpportunityType",
O."RUN_RATE_OPPORTUNITY_C",
/*Forecast*/
O."FORECAST_CATEGORY",
O."FORECAST_CATEGORY_NAME",
CASE WHEN O."STAGE_NAME" IN ('Qualification','Needs Analysis','Proposal / Quote','Negotiation','Commit','Quick Close','Submitted for Approval','Rejected','Closed Won') THEN 1 ELSE 0 END AS  "ForecastCategoryFunnelPipeline",
CASE WHEN O."STAGE_NAME" IN ('Negotiation','Commit','Quick Close','Submitted for Approval','Closed Won') THEN 1 ELSE 0 END AS  "ForecastCategoryFunnelBestCase",
CASE WHEN O."STAGE_NAME" IN ('Commit','Quick Close','Submitted for Approval','Closed Won') THEN 1 ELSE 0 END AS  "ForecastCategoryFunnelCommit",
CASE WHEN O."STAGE_NAME" = 'Closed Won' THEN 1 ELSE 0 END AS  "ForecastCategoryFunnelClosedWon",
/*Is Split*/
O."IS_SPLIT",
CASE WHEN O."IS_SPLIT" = '1' THEN 'Split'
  ELSE 'Not Split' END AS "IsSplitOrNotSplit",
O."ORDER_ID_C" AS "OrderId",
O."STATUS_C" AS "Status",
/*Hub Spot*/
O."HUB_SPOT_GENERATED_C",
CASE WHEN O."HUB_SPOT_GENERATED_C" = '1' THEN 'Hubspot Generated'
  ELSE 'Not Hubspot Generated' END AS "HubspotGenerated",
/*Affiliates*/
O."REFERRING_AFFILIATE_ACCOUNT_C",
O."AFFILIATE_OPPORTUNITY_C",
CASE WHEN (RT."NAME" IN ('Partner with Hosting','Partner Only')
  AND A."PARTNER_STATUS_C" = 'Signed Partner')
    OR (O."REFERRING_PARTNER_ACCOUNT_C" IS NOT NULL)
      THEN 'Partner'
  WHEN O."AFFILIATE_OPPORTUNITY_C" = 'true' THEN 'Affiliate'
  ELSE 'Digital'
    END AS "OppGenSource",
/*Partner*/
O."REFERRING_PARTNER_ACCOUNT_C",
A."PARTNER_TYPE_C" AS "SFAccountPartnerType",
CASE WHEN (RT."NAME" IN ('Partner with Hosting','Partner Only')
  AND A."PARTNER_STATUS_C" = 'Signed Partner')
    OR (O."REFERRING_PARTNER_ACCOUNT_C" IS NOT NULL)
  THEN 1 ELSE 0 END AS "IsPartner",
A."PARTNER_STATUS_C",
RT."NAME" AS "RecordTypeName",
sh."CREATED_DATE" AS "PartnerAgreementSignedDate",
CASE WHEN (RT."NAME" IN ('Partner with Hosting','Partner Only')
  AND A."PARTNER_STATUS_C" = 'Signed Partner'
  AND O."STAGE_NAME" = 'Closed Won')
    THEN 1 ELSE 0 END AS "PartnerOppsClosedWonCount",
CASE WHEN (RT."NAME" IN ('Partner with Hosting','Partner Only')
  AND A."PARTNER_STATUS_C" = 'Signed Partner'
  AND O."STAGE_NAME" = 'Closed Won')
    THEN O."NET_AMOUNT_C" ELSE 0 END AS "PartnerOppsClosedWonAmount",
CASE WHEN (RT."NAME" IN ('Partner with Hosting','Partner Only')
  AND A."PARTNER_STATUS_C" = 'Signed Partner'
  AND O."STAGE_NAME" IN ('Closed Lost','Closed Won')) THEN 1
    ELSE 0 END AS "PartnerOpportunitiesClosedNoOther",
CASE WHEN (O."REFERRING_PARTNER_ACCOUNT_C" IS NOT NULL
  AND O."STAGE_NAME" = 'Closed Won')
    THEN 1 ELSE 0 END AS "PartnerCustomerClosedWonOppsCount",
CASE WHEN O."REFERRING_PARTNER_ACCOUNT_C" IS NOT NULL
  AND O."STAGE_NAME" = 'Closed Won'
    THEN O."NET_AMOUNT_C" ELSE 0 END AS "PartnerCustomerClosedWonOppsAmount",
CASE WHEN (O."REFERRING_PARTNER_ACCOUNT_C" IS NOT NULL
  AND O."STAGE_NAME" IN ('Closed Lost','Closed Won')) THEN 1
    ELSE 0 END AS "PartnerCustomerOpportunitiesClosedNoOther",
/*Product Area*/
CASE WHEN O."PRODUCT_C" IN ('WordPress','Managed WordPress','MWP','MWCH Beginner','MWCH Standard','Dropshipping','Marketplace','WooCommerce','Magento','Expression Engine','MA - Other') THEN 'Managed Apps'
  WHEN O."PRODUCT_C" IN ('Cloud Dedicated','Dedicated','HIPAA','MES','Multiple','VPS','Cloud','MES Linux','MES Windows','Cloud Server') THEN 'Managed Hosting'
  WHEN O."PRODUCT_C" IN ('Premium Business Email','Backups','Cloud Sites','VMware','VMWare MultiTenant','MH - Other') THEN 'Managed Hosting'
  WHEN O."PRODUCT_C" IN ('Other','Unqualified') OR O."PRODUCT_C" IS NULL THEN 'Unknown'
    ELSE O."PRODUCT_C" END AS "ProductArea",
/*Products*/
CASE WHEN "PRODUCT_C" IN ('WordPress','Managed WordPress','MWP') THEN 'WordPress'
  WHEN "PRODUCT_C" IN ('MWCH Beginner','MWCH Standard','Dropshipping','Marketplace','WooCommerce') THEN 'WooCommerce'
  WHEN "PRODUCT_C" = 'Magento' THEN 'Magento'
  WHEN "PRODUCT_C" = 'Expression Engine' THEN 'Expression Engine'
  WHEN "PRODUCT_C" IN ('Dedicated','MES','Multiple','MES Linux','MES Windows') THEN 'Dedicated'
  WHEN "PRODUCT_C" = 'Cloud Server' THEN 'Cloud Server'
  WHEN "PRODUCT_C" = 'HIPAA' THEN 'HIPAA'
  WHEN "PRODUCT_C" IN ('Cloud','VPS') THEN 'VPS'
  WHEN "PRODUCT_C" = 'Cloud Dedicated' THEN 'Cloud Dedicated'
  WHEN "PRODUCT_C" IN ('Premium Business Email','Backups') THEN 'Add-Ons'
  WHEN "PRODUCT_C" IN ('Other') THEN 'Other'
  WHEN "PRODUCT_C" IN ('MA - Other') THEN 'MA - Other'
  WHEN "PRODUCT_C" IN ('MH - Other') THEN 'MH - Other'
  WHEN "PRODUCT_C" IN ('VMware','VMWare MultiTenant') THEN 'VMware'
  WHEN "PRODUCT_C" = 'Cloud Sites' THEN 'Cloud Sites'
  WHEN "PRODUCT_C" = 'Unqualified' THEN 'Unknown'
  WHEN "PRODUCT_C" IS NULL THEN 'Unknown'
    ELSE "PRODUCT_C" END AS "Product",
O."PRODUCT_C" AS "ProductLineOriginal",
/*Sales Channels*/
CASE WHEN E."division" LIKE 'Enterpris%' THEN 'Base Channel'
  WHEN E."division" = 'Partners' THEN 'Partner Channel'
  WHEN E."division" = 'Managed Apps' THEN 'Base Channel'
  WHEN E."division" LIKE '%Inbound%' THEN 'Inbound Channel'
  WHEN E."division" = 'Sales' AND E."department" LIKE '%Enterprise%' THEN 'Base Channel'
  WHEN E."division" = 'Sales' AND E."department" LIKE '%Inbound%' THEN 'Inbound Channel'
  WHEN E."division" = 'Sales' AND E."department" = 'Base Channel' THEN 'Base Channel'
  WHEN E."division" = 'Sales' AND E."department" LIKE '%Partner%' AND E."department" != 'Affiliate Partner' THEN 'Partner Channel'
  WHEN E."division" = 'BDC' THEN 'Base Channel'
  WHEN E."department" = 'Affiliate Partner' THEN 'Affiliate Channel'
  WHEN E."department" = 'EAM' THEN 'Base Channel'
  WHEN E."department" = 'Existing Logos' THEN 'Base Channel'
  WHEN E."department" = 'Hybrid Logos' THEN 'Inbound Channel'
  WHEN E."department" = 'New Logos' THEN 'Inbound Channel'
  WHEN (E."department" = 'Operations' OR E."division" = 'Operations') THEN 'Operations'
  WHEN (E."department" = 'Sales Enablement' OR E."division" = 'Sales Enablement') THEN 'Operations'
  WHEN E."department" = 'Sales Support Architect' THEN 'Operations'
  WHEN E."department" = 'Solutions Engineering' THEN 'Operations'
  WHEN E."department" = 'Solutions Support' THEN 'Operations'
  WHEN E."department" = 'Strategic' THEN 'Base Channel'
  WHEN E."department" LIKE 'BD%' THEN 'Base Channel'
  WHEN E."division" = 'Account Management' THEN 'Base Channel'
  WHEN E."department" = 'Targeted' THEN 'Inbound Channel'
  WHEN (E."department" ='Pipeline Team' OR E."department" = 'SDR') THEN 'Outbound Channel'
  WHEN (E."name" = 'Cassondra Clark' AND E."division" is null) THEN 'Inbound Channel'
    ELSE 'Other Channel' END AS "Sales Channels",
/*Employee SCD Objects*/
E."id" AS "EmployeeScdId",
E."name" AS "CREATED_BY_NAME",
owned."NAME" AS "OpportunityOwner",
E."division",
E."department" AS "department_original",
/*Historical Department Case*/
CASE WHEN E."department" IN ('Inbound Channel','MWP','New Logos','SDR','Web Inbound','Web Solutions','Targeted','Hybrid Logos','Strategic Solutions','Sales Enablement','Sales Support Architect') THEN 'Inbound Channel'
    WHEN E."department" IN ('AM','Base Channel','BDC','EAE','EAM','Enterprise Solutions','Existing Logos','Account Services') THEN 'Base Channel'
    WHEN E."department" IN ('MA Affiliate') THEN 'MA Affiliate'
    WHEN E."department" IN ('MA Partner Channel') THEN 'MA Partner Channel'
    WHEN E."department" IN ('MH Partner Channel','Solution Partners','Web Partner Solutions','Partner Solutions','Partner Channel','Strategic Partners') THEN 'MH Partner Channel'
    WHEN E."department" IN ('Pipeline Team') THEN 'Operations'
    WHEN E."department" IN ('Operations','Affiliate Partner') THEN 'Other'
    --All above are discontinued, all below are 9/1 Q team launch
    WHEN E."department" = 'MA Direct Sales' THEN 'MA Direct Sales'
    --MA Direct sales was active before 9/1 and is the only department still active after
    WHEN E."department" = 'Base Traditional' THEN 'Base Traditional'
    WHEN E."department" = 'Base Sophisticated' THEN 'Base Sophisticated'
    WHEN E."department" = 'Acquisition Traditional' THEN 'Acquisition Traditional'
    WHEN E."department" = 'Acquisition Sophisticated' THEN 'MA Direct Sales'
    WHEN E."department" = 'Q Team' THEN 'Q Team'
    WHEN E."department" = 'Referral Partner' THEN 'Referral Partner'
    WHEN E."department" = 'Reseller' THEN 'Reseller'
    ELSE E."department" END AS "DEPARTMENT",
CASE WHEN E."department" IN ('MA Direct Sales','Base Traditional','Base Sophisticated','Acquisition Traditional','Acquisition Sophisticated','Q Team','Referral Partner','Reseller') THEN 'Current'
    ELSE 'Legacy'
      END AS "DEPARTMENT_HIERARCHY",
CASE WHEN E."department" IN ('MA Affiliate','MA Direct Sales','MA Partner Channel','Referral Partner') THEN 'NX'
    WHEN E."department" = 'Q Team' THEN 'Q Team'
    WHEN E."department" NOT IN ('MA Affiliate','MA Direct Sales','MA Partner Channel','Referral Partner','Q Team') THEN 'LW'
        END AS "Company",
E."start_dtg"::date AS "EmployeeScdStartDate",
E."end_dtg"::date AS "EmployeeScdEndDate",
/*Date Objects*/
CURRENT_TIMESTAMP::timestamp AS "LastRefereshedDate",
D."QUARTER",
D."MONTH" AS "Month",
D."WEEK"||'-'||"YEAR" AS "WeekYear",
date_trunc('month', D."FULLDATE") AS "MonthYear",
D."QUARTER"||'-'||"YEAR" as "QuarterYear",
D."YEAR",
D."WEEK",
D."DAY",
D."DOW",
D."IS_WEEKDAY",
D."DOW_TEXT",
D."FULLDATE",
D."ISO_WEEK" AS "WeekNumber",
D."NON_ISO_WEEK_START" AS "WeekStart",
D."NON_ISO_WEEK_END" AS "WeekEnd"
FROM LZ_FIVETRAN.SALESFORCE."OPPORTUNITY" O
-- LEFT JOIN LZ_FIVETRAN.SALESFORCE."User" U
  -- ON O."OwnerId" = u."Id"
LEFT JOIN CREATED_DATE_FIX CDF
    ON CDF."OpportunityId" = O."ID"
LEFT JOIN ANALYST_PLAYGROUND.PUBLIC.DATES D
  ON CDF."CREATED_DATE"::DATE = D."FULLDATE"::DATE
LEFT JOIN LZ_PRODUCTION_ANALYSIS."scd"."employee_scd" E
  ON O."CREATED_BY_ID" = E."user_id"
  AND E."start_dtg" < CDF."CREATED_DATE"
  AND E."end_dtg" >= CDF."CREATED_DATE"
LEFT JOIN LZ_FIVETRAN.SALESFORCE.USER owned
  ON o."OWNER_ID" = owned."ID"
LEFT JOIN LZ_FIVETRAN.SALESFORCE."ACCOUNT" A
  ON O."ACCOUNT_ID" = A."ID"
LEFT JOIN LZ_FIVETRAN.SALESFORCE."RECORD_TYPE" RT
ON A."RECORD_TYPE_ID" = RT."ID"
LEFT JOIN signed_history SH
  ON O."ACCOUNT_ID" = sh."ACCOUNT_ID"
WHERE
COALESCE(E."name",'') NOT IN ('Corey Wagner', 'Chris White')
AND O."IS_DELETED" = 'false'
AND O."RUN_RATE_OPPORTUNITY_C" = 'false'
AND LOWER(COALESCE(O."NAME", '')) NOT LIKE '%lwtest%'
AND O."NET_AMOUNT_C" >= 0
AND COALESCE(O."TYPE",'') NOT LIKE '%Downgrade%'
AND LOWER(COALESCE(O."NAME", '')) != 'referral'
--Removing those Opportunities where the Owner is Mandy on the SF Team
AND E."name" != 'Mandy Shepard'
AND COALESCE(O."STAGE_NAME",'') NOT IN
    ('Closed - Duplicate','Marketing Generated Email & Outbound','Marketing Generated Email and Outbound',
       'Marketing Generated Outbound','Rejected','Mktg Opened - Sales Rejected','Marketing Generated')
AND O."ID" NOT IN (
SELECT DISTINCT
OFH."OPPORTUNITY_ID"
FROM LZ_FIVETRAN.SALESFORCE."OPPORTUNITY_FIELD_HISTORY" OFH
WHERE
OFH."FIELD" = 'StageName'
AND (OFH."OLD_VALUE" LIKE 'Marketing Generated%' AND OFH."NEW_VALUE" IN ('Closed Lost', 'Closed - Other')))
AND CDF."CREATED_DATE" <= CURRENT_DATE::DATE
--AND CDF."CREATED_DATE" >= '01-01-2019'
/*Test filter for the created date fix*/
-- AND SF_CREATED_DATE != CDF.CREATED_DATE
ORDER BY CDF."CREATED_DATE" DESC;
