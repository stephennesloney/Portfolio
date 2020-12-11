
CREATE OR REPLACE VIEW TF_DATA_WAREHOUSE.SALES_AND_MARKETING.LW_GA_SESSIONS_INTERACTIONS
AS
SELECT
z.*,
/*Product Area*/
CASE WHEN z."PRODUCT" IN ('WordPress', 'WooCommerce','Magento') THEN 'Managed Apps'
  WHEN z."PRODUCT" NOT IN ('WordPress', 'WooCommerce','Magento','Unknown','Other') THEN 'Managed Hosting' /*save for later IN ('ProductAddons', 'VPS', 'Dedicated','CloudSites','','')*/
  ELSE 'Unknown' END AS "PRODUCTAREA",
CASE WHEN z."PRODUCT" IN ('WordPress', 'WooCommerce','Magento') THEN 'Managed Apps'
  WHEN z."PRODUCT" NOT IN ('WordPress', 'WooCommerce','Magento','Unknown','Other') THEN 'Managed Hosting' /*save for later IN ('ProductAddons', 'VPS', 'Dedicated','CloudSites','','')*/
  ELSE 'Non-Product' END AS "PRODUCT_AREA_NON_PRODUCT"
FROM(
SELECT
"DATE"::date AS "DATE",
"SOURCE" AS "SOURCE",
"MEDIUM" AS "MEDIUM",
"CAMPAIGN" AS "CAMPAIGN",
"LANDING_PAGE_PATH" AS "LANDING_PAGE",
/*Channel Definitions*/
CASE
  WHEN "SOURCE" ILIKE '%webhostingtalk%'
        OR "SOURCE" = 'penton'
        OR "MEDIUM" = 'penton'
        OR "CAMPAIGN" ILIKE '%-pen-%'
            THEN 'Penton'
  WHEN "CAMPAIGN" ILIKE '%retargeting%' OR "CAMPAIGN" ILIKE '%-rtr-%'
        OR (SOURCE = 'adroll' AND MEDIUM = 'retargeting')
        OR (SOURCE = 'adroll' AND MEDIUM = 'adv')
        OR (SOURCE = 'adroll' AND MEDIUM = 'display')
          THEN 'Retargeting'
  WHEN "CAMPAIGN" ILIKE '%- High Intent%' OR "CAMPAIGN" ILIKE '%- Custom Intent%' OR "CAMPAIGN" ILIKE '%- Similar Audience%' OR "CAMPAIGN" ILIKE '%-dsp-%'
        OR ("SOURCE" = 'google' AND "MEDIUM" = 'display')
         /*These 3 rows below are to account for the System Definition of Display in GA*/
        OR "MEDIUM" ILIKE 'display'
        OR "MEDIUM" ILIKE 'cpm'
        OR "MEDIUM" ILIKE 'banner'
            THEN 'Display'
  /*The 1 row below are to account for the System Definition of Display in GA*/
  WHEN "MEDIUM" = 'cpc' OR "MEDIUM" = 'ppc' OR "MEDIUM" = 'paidsearch'
        OR ("SOURCE" = 'google' AND "MEDIUM" = 'cpc')
        OR ("SOURCE" = 'bing' AND "MEDIUM" = 'cpc')
        OR "CAMPAIGN" ILIKE '%-pdsr-%'
            THEN 'Paid Search'
  WHEN "MEDIUM" = 'affiliate' OR "CAMPAIGN" ILIKE '%-aff-%'
            THEN 'Affiliate'
  WHEN ("SOURCE" = 'rollworks' AND "MEDIUM" = 'abm')
        OR ("SOURCE" = 'rollworks' AND "MEDIUM" = 'display')
        OR ("SOURCE" = 'rollworks' AND "MEDIUM" = '(not set)')
            THEN 'ABM'
  WHEN ("SOURCE" = 'facebook' AND "MEDIUM" = 'paid-social')
        OR ("SOURCE" = 'linkedin' AND "MEDIUM" = 'paid-social')
        OR ("SOURCE" = 'twitter' AND "MEDIUM" = 'paid-social')
        OR "CAMPAIGN" ILIKE '%-pdso-%'
            THEN 'Paid Social'
  WHEN "MEDIUM" = 'email'
        OR "CAMPAIGN" ILIKE '%-eml-%'
            THEN 'Email'
  WHEN "MEDIUM" = 'organic'
        OR "SOURCE" = 'duckduckgo'
            THEN 'Organic Search'
  WHEN "SOURCE" = 'facebook'
        OR "SOURCE" = 'linkedin'
        OR "SOURCE" = 'twitter'
/*This lines below were added to match the GA system defined definition for social*/
        OR "MEDIUM" = 'social'
        OR "MEDIUM" = 'social-network'
        OR "MEDIUM" = 'social-media'
        OR "MEDIUM" = 'sm'
        OR "MEDIUM" = 'social network'
        OR "MEDIUM" = 'social media'
            THEN 'Organic Social'
  WHEN "MEDIUM" = 'referral'
            THEN 'Referral'
  WHEN ("SOURCE" = 'direct' AND "MEDIUM" = '(not set)')
        OR "MEDIUM" = '(none)'
            THEN 'Direct'
  ELSE 'Other' END AS "CHANNEL",
/*Identifies Affiliates*/
CASE WHEN "LANDING_PAGE_PATH" LIKE '%&_aw=1%'
  OR "LANDING_PAGE_PATH" LIKE '%&_cj=1%'
  OR "LANDING_PAGE_PATH" LIKE '%&_ir=1%'
  OR COALESCE(LOWER("MEDIUM"),'') = 'affiliate'
  OR COALESCE(LOWER("SOURCE"),'') = 'affiliate'
  OR "CAMPAIGN" = 'Homepage0'
  THEN 'Affiliate' ELSE 'Non-Affiliate'
  END AS "IS_AFFILIATE",
  /*Products*/
  CASE WHEN "LANDING_PAGE_PATH" LIKE '%cloud-dedicated%'
    THEN 'Cloud Dedicated'
  WHEN "LANDING_PAGE_PATH" LIKE '%/products/cloud-servers/‎%'
    THEN 'Cloud Server'
  WHEN "LANDING_PAGE_PATH" LIKE '%managed-hosting-buyers-guide%'
    OR "LANDING_PAGE_PATH" LIKE 'www.liquidweb.com/blog/shared-hosting-vs-vps-vs-dedicated/%'
    OR "LANDING_PAGE_PATH" LIKE 'go.liquidweb.com/windows-hosting/%'
    OR "LANDING_PAGE_PATH" LIKE '%/products/server-clusters/%'
    OR "LANDING_PAGE_PATH" LIKE '%/custom-solutions/high-availability-hosting/%'
    OR "LANDING_PAGE_PATH" LIKE '%/custom-solutions/high-performance-hosting/%'
    OR "LANDING_PAGE_PATH" LIKE '%/solutions/pci-compliance/%'
    OR "LANDING_PAGE_PATH" LIKE '%custom%solutions%'
    /*adding because people and fucking processes*/
    OR "LANDING_PAGE_PATH" LIKE '%/black-friday-2019%'
    OR "LANDING_PAGE_PATH" LIKE '%/year-end-2019/%'
    OR "LANDING_PAGE_PATH" LIKE '%/solutions/pci-compliance/%'
    OR "LANDING_PAGE_PATH" LIKE '%custom%solutions%'
    THEN 'Dedicated'
  WHEN ("LANDING_PAGE_PATH" LIKE '%dedi%'
      OR "LANDING_PAGE_PATH" LIKE '%hosting%happiness%'
      OR "LANDING_PAGE_PATH" LIKE '%helpful%'
      OR "LANDING_PAGE_PATH" LIKE '%advice38%'
      OR "LANDING_PAGE_PATH" LIKE '%htgod38%'
      OR "LANDING_PAGE_PATH" LIKE '%managed-hosting%'
      OR "LANDING_PAGE_PATH" LIKE '%fastwin35%')
--       OR "LANDING_PAGE_PATH" LIKE '%private-cloud%')
    AND "LANDING_PAGE_PATH" NOT LIKE '%woo%'
  THEN 'Dedicated'
  WHEN "LANDING_PAGE_PATH" LIKE '%/products/private-cloud/%'
  THEN 'VMware'
 WHEN "LANDING_PAGE_PATH" LIKE '%/solutions/hipaa-compliant-hosting/%'
      OR "LANDING_PAGE_PATH" LIKE '%hipaa%'
    THEN 'HIPAA'
  WHEN ("LANDING_PAGE_PATH" LIKE '%vps%'
    OR "LANDING_PAGE_PATH" LIKE '%htgov50%'
    OR "LANDING_PAGE_PATH" LIKE '%nowuk50%')
    AND ("LANDING_PAGE_PATH" NOT LIKE '%mwp%' OR "LANDING_PAGE_PATH" NOT LIKE '%wordpress%')
    THEN 'VPS'
  WHEN "LANDING_PAGE_PATH" LIKE '%cloudsites.liquidweb.com%'
    OR "LANDING_PAGE_PATH" LIKE 'go.liquidweb.com/fastcloud38/%'
    OR "LANDING_PAGE_PATH" LIKE '%cloud-sites%'
    OR "LANDING_PAGE_PATH" LIKE '%cloudsites%'
    THEN 'Cloud Sites'
  WHEN "LANDING_PAGE_PATH" LIKE '%woo%'
    OR "LANDING_PAGE_PATH" LIKE '%commerce%'
    OR "LANDING_PAGE_PATH" LIKE '%mwch%'
    OR "LANDING_PAGE_PATH" LIKE 'go.liquidweb.com/managed-hosting-for-woocommerce/%'
    THEN 'WooCommerce'
  WHEN "LANDING_PAGE_PATH" LIKE '%word%press%'
    OR "LANDING_PAGE_PATH" LIKE '%mwp%'
    OR "LANDING_PAGE_PATH" LIKE '%14-strategies%'
    OR "LANDING_PAGE_PATH" LIKE '%amazon-ebook%'
    OR "LANDING_PAGE_PATH" LIKE 'www.liquidweb.com/blog/liquid-web-vs-wpengine/%'
    OR "LANDING_PAGE_PATH" LIKE '%/managed-wordpress/%'
    OR "LANDING_PAGE_PATH" LIKE '%/liquid-web-vs-wpengine/%'
    OR "LANDING_PAGE_PATH" LIKE '%app.%liquidwebsites.com%'
    THEN 'WordPress'
  WHEN "LANDING_PAGE_PATH" LIKE '%/products/magento-cloud/%'
      OR "LANDING_PAGE_PATH" LIKE '%magento%'
        THEN 'Magento'
  WHEN "LANDING_PAGE_PATH" LIKE '%add-ons%'
    OR "LANDING_PAGE_PATH" = 'Premium Business Email'
    THEN 'Add-Ons'
  ELSE 'Other' END AS "PRODUCT",
/*Web Product is used for when you need all LW sessions to be MH*/
'Dedicated' AS "WebProduct",
 /*Page Type*/
CASE WHEN "LANDING_PAGE_PATH" LIKE '%/products/private-cloud/%'
OR "LANDING_PAGE_PATH" LIKE '%/products/server-clusters/%'
OR "LANDING_PAGE_PATH" LIKE '%/custom-solutions/high-availability-hosting/%'
OR "LANDING_PAGE_PATH" LIKE '%/custom-solutions/high-performance-hosting/%'
OR "LANDING_PAGE_PATH" LIKE '%/solutions/hipaa-compliant-hosting/%'
OR "LANDING_PAGE_PATH" LIKE '%/solutions/pci-compliance/%'
  THEN 'TRUE' ELSE 'FALSE' END AS "COMMA_DEAL_PAGE",
CASE WHEN "LANDING_PAGE_PATH" LIKE '%com/products/%'
    OR "LANDING_PAGE_PATH" LIKE '%com/products-catalog/%'
    OR "LANDING_PAGE_PATH" LIKE '%com/Dedicated/%'
     THEN 'Product'
  WHEN "LANDING_PAGE_PATH" LIKE '%com/kb/%'
    THEN 'KB'
  WHEN "LANDING_PAGE_PATH" LIKE '%/blog/%'
    THEN 'Blog'
  WHEN "LANDING_PAGE_PATH" LIKE '%www.liquidweb.com/'
    AND "LANDING_PAGE_PATH" NOT LIKE '%www.liquidweb.com/?s=%'
    THEN 'HomePage'
  WHEN "LANDING_PAGE_PATH" LIKE '%/blog/%'
    THEN 'Blog'
  WHEN "LANDING_PAGE_PATH" LIKE '%help.liquidweb.com%'
    THEN 'Help'
  WHEN "LANDING_PAGE_PATH" LIKE '%hub.liquidweb.com/%'
    THEN 'Hub'
  WHEN "LANDING_PAGE_PATH" LIKE '%cart.liquidweb.com%'
    THEN 'Cart'
  WHEN "LANDING_PAGE_PATH" LIKE '%manage.liquidweb.com%'
    THEN 'Manage'
    /*adding new manage 20191209*/
    WHEN "LANDING_PAGE_PATH" LIKE '%my.liquidweb.com%'
        THEN 'My'
  WHEN "LANDING_PAGE_PATH" LIKE '%go.liquidweb.com/%'
    THEN 'Go'
  WHEN "LANDING_PAGE_PATH" LIKE '%com/support/%'
    THEN 'Support'
  WHEN "LANDING_PAGE_PATH" LIKE '%com/solutions/%'
    OR "LANDING_PAGE_PATH" LIKE '%/custom-solutions/%'
    AND "LANDING_PAGE_PATH" NOT LIKE '%solutions/reseller%'
    THEN 'Solutions'
  WHEN ("LANDING_PAGE_PATH" LIKE '%partner%'
    OR "LANDING_PAGE_PATH" LIKE '%reseller%'
    OR "LANDING_PAGE_PATH" LIKE '%affiliate%'
    OR "LANDING_PAGE_PATH" LIKE 'go.liquidweb.com/web-professional-program-creators/%')
    AND "LANDING_PAGE_PATH" NOT LIKE '%/s/case/%'
    THEN 'Partner'
     ELSE 'Other' END AS "PAGE_TYPE",
/*dates*/
d."NON_ISO_WEEK_START" AS "WEEK_START",
d."FULLDATE" AS "FULL_DATE",
DATE_TRUNC('MONTH',d."FULLDATE") AS "MONTH_YEAR",
/*Removing because we hit the limit of metrics in fivetran*/ 
--SUM("USERS") AS "USERS",
--SUM("NEW_USERS") AS "NEW_USERS",
SUM("SESSIONS") AS "SESSIONS",
--Goal 1 turned off in GA
SUM("GOAL_1_COMPLETIONS") AS "DEMO_SCHEDULED",
SUM("GOAL_2_COMPLETIONS") AS "CLICK_TO_CALL",
SUM("GOAL_5_COMPLETIONS") AS "SALES_CHATS",
SUM("GOAL_6_COMPLETIONS") AS "W2L_FORM_LEAD",
--Goal 8 turned off in GA
SUM("GOAL_8_COMPLETIONS") AS "W2L_FORM_PARTNER",
SUM("GOAL_11_COMPLETIONS") AS "CART_START",
SUM("GOAL_12_COMPLETIONS") AS "CART_COMPLETION",
SUM("TRANSACTIONS") AS "TRANSACTIONS"
FROM LZ_FIVETRAN.LW_GA_INTERACTIONS.LW_GA_INTERACTIONS_BY_SOURCE ga
LEFT JOIN ANALYST_PLAYGROUND.PUBLIC.DATES d
  ON ga."DATE"::DATE = d."FULLDATE"::DATE
GROUP BY
"DATE"::date,
"SOURCE",
"MEDIUM",
"CAMPAIGN",
"LANDING_PAGE_PATH",
d."NON_ISO_WEEK_START",
d."FULLDATE")z;
