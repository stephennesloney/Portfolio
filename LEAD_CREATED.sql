CREATE OR REPLACE VIEW TF_DATA_WAREHOUSE.SALES_AND_MARKETING.LEAD_CREATED
AS
SELECT
l."ID" AS "LEAD_ID",
l."LW_ACCOUNT_NUMBER_C",
l."IS_DELETED",
l."CREATED_DATE"::DATE AS "CREATED_DATE",
l."LEAD_SOURCE",
l."ORIGINAL_LEAD_SOURCE_C",
l."STATUS",
CASE WHEN (l."LEAD_CREATE_MEDIUM_C" = 'Other' AND l."LEAD_SOURCE" = 'Event') THEN 'Event'
  ELSE l."LEAD_CREATE_MEDIUM_C"
  END AS "LEAD_CREATE_MEDIUM_C",
l."PRODUCT_LINE_C",
l."CAMPAIGN_C",
l."OWNER_ID",
l."NAME",
l."LOGO_TYPE_C",
l."LOST_STATUS_DETAILS_C",
l."LOST_STATUS_DETAILS_2_C",
l."LOST_STATUS_DETAILS_3_C",
l."CREATED_BY_ID",
l."LAST_ACTIVITY_DATE",
l."LAST_ACTIVITY_AGE_C",
l."LAST_MODIFIED_DATE",
l."SOURCE_C",
l."OTHER_SOURCE_C",
l."SPECIFIC_SOURCE_C",
l."LEAD_SPECIFIC_SOURCE_DETAIL_C",
/*Conversion Metrics*/
CASE WHEN l."STATUS" = 'Opportunity - Converted' THEN 1 ELSE 0 END AS "LEADS_CONVERTED",
CASE WHEN l."STATUS" IN ('Send to Nurture', 'Opportunity - Converted','Lost') THEN 1 ELSE 0 END AS "LEADS_CONVERSION_DENOMINATOR",
l."CONVERTED_OPPORTUNITY_ID",
l."IS_CONVERTED",
l."CONVERTED_DATE",
l."REFERRING_PARTNER_ACCOUNT_C",
1 AS "LEADS",
/*Partner Counts*/
CASE WHEN rt."NAME" = 'Partner'
  THEN 1 ELSE 0 END AS "PARTNER_LEAD",
CASE WHEN rt."NAME" = 'PARTNER CUSTOMER'
  THEN 1 ELSE 0 END AS "PARTNER_CUSTOMER_LEAD",
/*Product Area*/
CASE WHEN l."PRODUCT_LINE_C" IN ('Managed WooCommerce','MWCH Beginner','WordPress','Managed WordPress','MWCH Standard','Platform (MWP or CS)','Dropshipping','Marketplace','WooCommerce','Expression Engine','Magento','MA - Other') THEN 'Managed Apps'
  WHEN l."PRODUCT_LINE_C" IN ('MES','Cloud Sites','Multiple','Cloud Dedicated','Dedicated','HIPAA','Premium Business Email','Cloud VPS','VPS','VMware','MES Linux','MES Windows','Cloud Server','VMWare MultiTenant') THEN 'Managed Hosting'
  WHEN l."PRODUCT_LINE_C" = 'Other' OR l."PRODUCT_LINE_C" IS NULL THEN 'Unknown'
    ELSE  l."PRODUCT_LINE_C" END AS "PRODUCTAREA",
/*Product*/
CASE WHEN "PRODUCT_LINE_C" IN ('WordPress','Managed WordPress','MWP','Platform (MWP or CS)') THEN 'WordPress'
  WHEN "PRODUCT_LINE_C" IN ('MWCH Beginner','MWCH Standard','Managed WooCommerce','Dropshipping','Marketplace','WooCommerce') THEN 'WooCommerce'
  WHEN "PRODUCT_LINE_C" = 'Magento' THEN 'Magento'
  WHEN "PRODUCT_LINE_C" = 'Expression Engine' THEN 'Expression Engine'
  WHEN "PRODUCT_LINE_C" IN ('Dedicated','MES','Multiple','MES Linux','MES Windows') THEN 'Dedicated'
  WHEN "PRODUCT_LINE_C" = 'Cloud Server' THEN 'Cloud Server'
  WHEN "PRODUCT_LINE_C" IN ('Cloud Dedicated','Private Parent') THEN 'Cloud Dedicated'
  WHEN "PRODUCT_LINE_C" = 'Cloud Sites' THEN 'Cloud Sites'
  WHEN "PRODUCT_LINE_C" IN ('VPS','Cloud VPS','Cloud') THEN 'VPS'
  WHEN "PRODUCT_LINE_C" = 'Premium Business Email' THEN 'Add-Ons'
  WHEN "PRODUCT_LINE_C" = 'HIPAA' THEN 'HIPAA'
  WHEN "PRODUCT_LINE_C" IN ('VMware','VMWare MultiTenant') THEN 'VMware'
  WHEN "PRODUCT_LINE_C" = 'Other' THEN 'Other'
  WHEN "PRODUCT_LINE_C" = 'MA - Other' THEN 'MA - Other'
  WHEN "PRODUCT_LINE_C" IS NULL THEN 'Unknown'
    ELSE "PRODUCT_LINE_C" END AS "PRODUCT",
"PRODUCT_LINE_C" AS "ProductLineOriginal",
l."PRODUCT_LINE_OTHER_C",
/*Lead Grouping*/
CASE WHEN l."STATUS" IN ('Marketing Qualified','Prospect','Connection','Lost','Opportunity - Converted') THEN 1
    ELSE 0 END AS "MarketingQualifiedLead",
CASE WHEN l."STATUS" IN ('Prospect','Connection','Lost','Opportunity - Converted') THEN 1
    ELSE 0 END AS "ProspectLead",
CASE WHEN l."STATUS" IN ('Connection','Lost','Opportunity - Converted') THEN 1
    ELSE 0 END AS "ConnectedLead",
CASE WHEN l."STATUS" IN ('Opportunity - Converted') THEN 1
    ELSE 0 END AS "ConvertedToOpportunity",
CASE WHEN l."STATUS" IN ('Lost') THEN 1
    ELSE 0 END AS "LOST_LEAD",
CASE WHEN l."LEAD_SOURCE" IN ('Web Lead - Form','Website') THEN 'Website'
  WHEN l."LEAD_SOURCE" IN ('Organic Search','Seach Engine') THEN 'Organic Search'
  WHEN l."LEAD_SOURCE" = 'Direct' THEN 'Direct'
  WHEN l."LEAD_SOURCE" IN ('Media Buys','Advertisement','Paid Search','Social Advertising','Paid Social') THEN 'Paid Advertisement'
  WHEN l."LEAD_SOURCE" IN ('Email','Email Marketing') THEN 'Email'
  WHEN l."LEAD_SOURCE" = 'Event' THEN 'Event'
  WHEN l."LEAD_SOURCE" = 'Referal' THEN 'Referal'
    ELSE 'Other' END AS "LEAD_SOURCE_GROUPED",
/*Lead Source Buckets*/
CASE WHEN l."REFERRING_PARTNER_ACCOUNT_C" IS NOT NULL
    OR rt."NAME" IN ('Partner','Partner Customer','Partner Application')
    OR o."REFERRING_PARTNER_ACCOUNT_C" IS NOT NULL
      THEN 'Partner'
  WHEN (z."GOOGLE_PAGE_URL_C" LIKE '%_aw=1%'
    OR z."GOOGLE_PAGE_URL_C" LIKE '%_cj=1%'
    OR z."GOOGLE_PAGE_URL_C" LIKE '%_ir=1%'
    OR l."DRIFT_CONVERSATION_STARTED_PAGE_URL_C" LIKE '%_aw=1%'
    OR l."DRIFT_CONVERSATION_STARTED_PAGE_URL_C" LIKE '%_cj=1%'
    OR l."DRIFT_CONVERSATION_STARTED_PAGE_URL_C" LIKE '%_ir=1%'
    OR z."GOOGLE_PAGE_URL_C" LIKE '%utm_medium=affiliate%'
    OR l."DRIFT_CONVERSATION_STARTED_PAGE_URL_C" LIKE '%utm_medium=affiliate%'
    -- OR o."REFERRING_AFFILIATE_ACCOUNT_C" IS NOT NULL
    -- OR o."AFFILIATE_OPPORTUNITY_C" = 'true'
    )
      THEN 'Affiliate'
  ELSE 'Digital/Targeted'
  END AS "LEAD_GEN_SOURCE",
E."DEPARTMENT" AS "department_original",
/*Historical Department Case*/
CASE WHEN E."DEPARTMENT" IN ('Inbound Channel','MWP','New Logos','SDR','Web Inbound','Web Solutions','Targeted','Hybrid Logos','Strategic Solutions','Sales Enablement','Sales Support Architect') THEN 'Inbound Channel'
    WHEN E."DEPARTMENT" IN ('AM','Base Channel','BDC','EAE','EAM','Enterprise Solutions','Existing Logos','Account Services') THEN 'Base Channel'
    WHEN E."DEPARTMENT" IN ('MA Affiliate') THEN 'MA Affiliate'
    WHEN E."DEPARTMENT" IN ('MA Partner Channel') THEN 'MA Partner Channel'
    WHEN E."DEPARTMENT" IN ('MH Partner Channel','Solution Partners','Web Partner Solutions','Partner Solutions','Partner Channel','Strategic Partners') THEN 'MH Partner Channel'
    WHEN E."DEPARTMENT" IN ('Pipeline Team') THEN 'Operations'
    WHEN E."DEPARTMENT" IN ('Operations','Affiliate Partner') THEN 'Other'
    --All above are discontinued, all below are 9/1 Q team launch
    WHEN E."DEPARTMENT" = 'MA Direct Sales' THEN 'MA Direct Sales'
    --MA Direct sales was active before 9/1 and is the only department still active after
    WHEN E."DEPARTMENT" = 'Base Traditional' THEN 'Base Traditional'
    WHEN E."DEPARTMENT" = 'Base Sophisticated' THEN 'Base Sophisticated'
    WHEN E."DEPARTMENT" = 'Acquisition Traditional' THEN 'Acquisition Traditional'
    WHEN E."DEPARTMENT" = 'Acquisition Sophisticated' THEN 'MA Direct Sales'
    WHEN E."DEPARTMENT" = 'Q Team' THEN 'Q Team'
    WHEN E."DEPARTMENT" = 'Referral Partner' THEN 'Referral Partner'
    WHEN E."DEPARTMENT" = 'Reseller' THEN 'Reseller'
    ELSE E."DEPARTMENT" END AS "DEPARTMENT",
CASE WHEN E."DEPARTMENT" IN ('MA Direct Sales','Base Traditional','Base Sophisticated','Acquisition Traditional','Acquisition Sophisticated','Q Team','Referral Partner','Reseller') THEN 'Current'
    ELSE 'Legacy'
      END AS "DEPARTMENT_HIERARCHY",
CASE WHEN E."DEPARTMENT" IN ('MA Affiliate','MA Direct Sales','MA Partner Channel','Referral Partner') THEN 'NX'
    WHEN E."DEPARTMENT" = 'Q Team' THEN 'Q Team'
    WHEN E."DEPARTMENT" NOT IN ('MA Affiliate','MA Direct Sales','MA Partner Channel','Referral Partner','Q Team') THEN 'LW'
        END AS "Company",
/*Sales Channel replaced by Department*/
-- /*Sales Channels*/
-- CASE WHEN e."division" LIKE 'Enterpris%' THEN 'Base Channel'
--   WHEN E."division" = 'Partners' THEN 'Partner Channel'
--   WHEN E."division" = 'Managed Apps' THEN 'Base Channel'
--   WHEN E."division" LIKE '%Inbound%' THEN 'Inbound Channel'
--   WHEN E."division" = 'Sales' AND E."DEPARTMENT" LIKE '%Enterprise%' THEN 'Base Channel'
--   WHEN E."division" = 'Sales' AND E."DEPARTMENT" LIKE '%Inbound%' THEN 'Inbound Channel'
--   WHEN E."division" = 'Sales' AND E."DEPARTMENT" = 'Base Channel' THEN 'Base Channel'
--   WHEN E."division" = 'Sales' AND E."DEPARTMENT" LIKE '%Partner%' AND E."DEPARTMENT" != 'Affiliate Partner' THEN 'Partner Channel'
--   WHEN E."division" = 'BDC' THEN 'Base Channel'
--   WHEN E."DEPARTMENT" = 'Affiliate Partner' THEN 'Affiliate Channel'
--   WHEN E."DEPARTMENT" = 'EAM' THEN 'Base Channel'
--   WHEN E."DEPARTMENT" = 'Existing Logos' THEN 'Base Channel'
--   WHEN E."DEPARTMENT" = 'Hybrid Logos' THEN 'Inbound Channel'
--   WHEN E."DEPARTMENT" = 'New Logos' THEN 'Inbound Channel'
--   WHEN (E."DEPARTMENT" = 'Operations' OR E."division" = 'Operations') THEN 'Operations'
--   WHEN (E."DEPARTMENT" = 'Sales Enablement' OR E."division" = 'Sales Enablement') THEN 'Operations'
--   WHEN E."DEPARTMENT" = 'Sales Support Architect' THEN 'Operations'
--   WHEN E."DEPARTMENT" = 'Solutions Engineering' THEN 'Operations'
--   WHEN E."DEPARTMENT" = 'Solutions Support' THEN 'Operations'
--   WHEN E."DEPARTMENT" = 'Strategic' THEN 'Base Channel'
--   WHEN E."DEPARTMENT" LIKE 'BD%' THEN 'Base Channel'
--   WHEN E."division" = 'Account Management' THEN 'Base Channel'
--   WHEN E."DEPARTMENT" = 'Targeted' THEN 'Inbound Channel'
--   WHEN (E."DEPARTMENT" ='Pipeline Team' OR E."DEPARTMENT" = 'SDR') THEN 'Outbound Channel'
--   WHEN (E."name" = 'Cassondra Clark' AND E."division" is null) THEN 'Inbound Channel'
--     ELSE 'Other Channel' END AS "SALES_CHANNELS",
owner."NAME" AS "LEAD_OWNER",
E."USER_ID"::VARCHAR AS "EmployeeScdId",
E."NAME" AS "CREATED_BY_NAME",
E."DIVISION" AS "DIVISION",
rt."ID" AS "RECORD_TYPE_ID",
rt."NAME" AS "RECORD_TYPE_NAME",
date_trunc('month', l."CREATED_DATE")::Date AS "START_OF_MONTH",
d."NON_ISO_WEEK_START" AS "WEEK_START",
d."FULLDATE" AS "FULL_DATE",
DATE_TRUNC('MONTH',d."FULLDATE") AS "MONTH_YEAR"
FROM LZ_FIVETRAN.SALESFORCE.LEAD l
/*historical employees*/
-- LEFT JOIN LZ_PRODUCTION_ANALYSIS."scd"."employee_scd" E
--   ON l."CREATED_BY_ID" = E."user_id"
--   AND E."start_dtg" < l."CREATED_DATE"
--   AND E."end_dtg" >= l."CREATED_DATE"
LEFT JOIN TF_UNIFIED.PUBLIC.UNIFIED_EMPLOYEE_SCD_BARE E
  ON l."CREATED_BY_ID" = E."USER_ID"
  AND l."CREATED_DATE"::DATE = E."FULLDATE"::DATE
LEFT JOIN ANALYST_PLAYGROUND.PUBLIC.DATES d
  ON l."CREATED_DATE"::DATE = d."FULLDATE"::DATE
LEFT JOIN LZ_FIVETRAN.SALESFORCE.USER owner
  ON l."OWNER_ID" = owner."ID"
LEFT JOIN LZ_FIVETRAN.SALESFORCE.RECORD_TYPE rt
  ON rt."ID" = l."RECORD_TYPE_ID"
LEFT JOIN LZ_FIVETRAN.SALESFORCE.OPPORTUNITY o
  ON l."CONVERTED_OPPORTUNITY_ID" = o."ID"
LEFT JOIN (SELECT lct.*,
           ROW_NUMBER () OVER (PARTITION BY lct."LEAD_ID" ORDER BY lct."REQUEST_TIME") AS "rank"
   FROM LZ_FIVETRAN.SALESFORCE.LIVE_CHAT_TRANSCRIPT lct
   ) z
 ON l."ID" = z."LEAD_ID" AND z."rank" = 1
WHERE l."IS_DELETED" = 'false'
AND COALESCE(l."STATUS",'') NOT IN ('Marketing Generated','Disqualified','Prospect','Converted - Existing Customer','Converted - Duplicate')
AND COALESCE(rt."NAME",'') != 'Cold Prospect'
AND COALESCE(l."FIRST_NAME",'') NOT LIKE '%LWTEST%'
AND COALESCE(l."LAST_NAME",'') NOT LIKE '%LWTEST%'
AND COALESCE(l."COMPANY",'') NOT LIKE '%LWTEST%'
AND COALESCE(l."LOGO_TYPE_C",'') != 'Existing'
AND COALESCE(l."LEAD_SOURCE",'') != 'Dev-Leads'
AND COALESCE(l."LOST_STATUS_DETAILS_C",'') NOT IN ('Duplicate', 'Spam', 'Unassisted')
AND COALESCE(l."EMAIL",'') NOT LIKE '%liquidweb.com%'
AND COALESCE(l."EMAIL",'') NOT LIKE '%LWTEST%'
AND ((l."BULK_UPLOAD_C" = 'false')
  OR (l."BULK_UPLOAD_C" = 'true' AND coalesce(l."STATUS",'') != 'Marketing Generated')
  OR (l."BULK_UPLOAD_C" = 'true' AND coalesce(l."STATUS",'') != 'Prospect'))
AND COALESCE(l."PRODUCT_LINE_C",'') != 'Unqualified'
/*removing Nexcess leads*/
--AND rt."NAME" IN ('Nexcess Sales','Nexcess')
AND l."ID" NOT IN (
  SELECT DISTINCT
  "LEAD_ID"
  FROM LZ_FIVETRAN.SALESFORCE.LEAD_HISTORY
  WHERE "FIELD" = 'Status'
  AND (("OLD_VALUE" = 'Marketing Generated' AND "NEW_VALUE" = 'Lost')
    OR ("OLD_VALUE" = 'Marketing Generated' AND "NEW_VALUE" = 'Send to Nurture')
    OR ("OLD_VALUE" = 'Prospect' AND "NEW_VALUE" = 'Send to Nurture')))
ORDER BY date_trunc('month', l."CREATED_DATE") DESC;
