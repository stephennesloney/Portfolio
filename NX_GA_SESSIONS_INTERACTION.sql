CREATE OR REPLACE VIEW TF_DATA_WAREHOUSE.SALES_AND_MARKETING.NX_GA_SESSIONS_INTERACTIONS
AS
SELECT
z.*,
/*Product Area*/
CASE WHEN z."PRODUCT" IN ('Magento','WooCommerce','WordPress','Flex') THEN 'Managed Apps'
  --WHEN z."PRODUCT" IN ('Magento','WooCommerce','WordPress','Drupal','BigCommerce','Sylius','ExpressionEngine','CraftCMS','OroCRM') THEN 'Managed Applications'
  --WHEN z."PRODUCT" NOT IN ('WordPress', 'WooCommerce','Unknown','Other') THEN 'Managed Hosting' /*save for later IN ('ProductAddons', 'VPS', 'Dedicated','CloudSites','','')*/
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
/*
Old Products Grouping
CASE WHEN "LANDING_PAGE_PATH" LIKE '%nexcess.net/magento/%' THEN 'Magento'
    WHEN "LANDING_PAGE_PATH" LIKE '%nexcess.net/woocommerce/%' THEN 'WooCommerce'
    WHEN "LANDING_PAGE_PATH" LIKE '%nexcess.net/wordpress/%' THEN 'WordPress'
    WHEN "LANDING_PAGE_PATH" LIKE '%nexcess.net/drupal/%' THEN 'Drupal'
    WHEN "LANDING_PAGE_PATH" LIKE '%nexcess.net/bigcommerce/%' THEN 'BigCommerce'
    WHEN "LANDING_PAGE_PATH" LIKE '%nexcess.net/sylius/%' THEN 'Sylius'
    WHEN "LANDING_PAGE_PATH" LIKE '%nexcess.net/expressionengine/%' THEN 'ExpressionEngine'
    WHEN "LANDING_PAGE_PATH" LIKE '%nexcess.net/craft-cms/%' THEN 'CraftCMS'
    WHEN "LANDING_PAGE_PATH" LIKE '%nexcess.net/oro/%' THEN 'OroCRM'
    ELSE 'Other' END AS "PRODUCT",
*/
/*Products*/
CASE WHEN "LANDING_PAGE_PATH" LIKE '%nexcess.net/magento/%' THEN 'Magento'
    WHEN "LANDING_PAGE_PATH" LIKE '%nexcess.net/woocommerce/%' THEN 'WooCommerce'
    WHEN "LANDING_PAGE_PATH" LIKE '%nexcess.net/wordpress/%' THEN 'WordPress'
    WHEN "LANDING_PAGE_PATH" LIKE '%nexcess.net/drupal/%'
      OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/bigcommerce/%'
      OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/sylius/%'
      OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/expressionengine/%'
      OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/craft-cms/%'
      OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/oro/%'
        THEN 'Flex'
    ELSE 'Unknown'
    END AS "PRODUCT",
/*Web Product is used for when you need all NX sessions to be MA*/
'WordPress' AS "WebProduct",
 /*Page Type*/
CASE WHEN "LANDING_PAGE_PATH" LIKE '%nexcess.net/magento/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/woocommerce/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/wordpress/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/drupal/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/bigcommerce/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/sylius/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/expressionengine/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/craft-cms/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/oro/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/cloud/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/enterprise-hosting/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/michigan-colocation/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/unmanaged-hosting/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/domain-registration/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/ssl-certificates/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/content-delivery-network/%'
      THEN 'Solutions'
  WHEN "LANDING_PAGE_PATH" LIKE '%nexcess.net/pricing/cloud-pricing/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/pricing/dedicated-servers/%'
     THEN 'Pricing'
  WHEN "LANDING_PAGE_PATH" LIKE '%nexcess.net/cloud/auto-scaling/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/cloud/accelerator/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/cloud/application-stack/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/cloud/elasticsearch/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/money-back-guarantee/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/support/beyond-management/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/cloud/development-sites/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/support/dns/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/compliance/pci-hosting/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/compliance/ssae-18/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/compliance/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/content-delivery-network/%'
    OR "LANDING_PAGE_PATH" LIKE '%nexcess.net/support/migration/%'
      THEN 'Features'
  WHEN "LANDING_PAGE_PATH" LIKE '%nexcess.net/partners/%'
    OR "LANDING_PAGE_PATH" LIKE '%/partners/%'
      THEN 'Partner'
  WHEN "LANDING_PAGE_PATH" LIKE '%portal.nexcess.net/%'
    THEN 'Portal'
  WHEN "LANDING_PAGE_PATH" LIKE '%help.nexcess.net%'
    THEN 'Help'
  WHEN "LANDING_PAGE_PATH" LIKE '%nexcess.net/support/%'
    THEN 'Support'
  WHEN "LANDING_PAGE_PATH" LIKE '%blog.nexcess.net%'
    THEN 'Blog'
  WHEN "LANDING_PAGE_PATH" LIKE 'www.nexcess.net/'
    OR "LANDING_PAGE_PATH" LIKE '/www.nexcess.netwww.nexcess.net/'
      THEN 'HomePage'
  WHEN "LANDING_PAGE_PATH" LIKE '%go.nexcess.net/%'
    THEN 'Go'
  WHEN "LANDING_PAGE_PATH" LIKE '%shop.nexcess.net/%'
    THEN 'Shop'
  WHEN "LANDING_PAGE_PATH" LIKE '%explore.nexcess.net/%'
    THEN 'Explore'
  WHEN "LANDING_PAGE_PATH" LIKE '%order.nexcess.net/%'
    THEN 'Order'
      ELSE 'Other' END AS "PAGE_TYPE",
/*dates*/
d."NON_ISO_WEEK_START" AS "WEEK_START",
d."FULLDATE" AS "FULL_DATE",
DATE_TRUNC('MONTH',d."FULLDATE") AS "MONTH_YEAR",
SUM("USERS") AS "USERS",
SUM("NEW_USERS") AS "NEW_USERS",
SUM("SESSIONS") AS "SESSIONS",
SUM("GOAL_14_COMPLETIONS") AS "SALES_CHATS",
SUM("GOAL_11_COMPLETIONS") AS "CART_START",
SUM("GOAL_1_COMPLETIONS") AS "CART_COMPLETIONS",
SUM("GOAL_2_COMPLETIONS") AS "FREE_TRIAL_CART_COMPLETION",
SUM("GOAL_6_COMPLETIONS") AS "W2L_FORM_LEAD",
SUM("GOAL_3_COMPLETIONS") AS "CLICK_TO_CALL"
/* LW goals for comparison
SUM("GOAL_5_COMPLETIONS") AS "SALES_CHATS",
SUM("GOAL_6_COMPLETIONS") AS "W2L_FORM_LEAD",
SUM("GOAL_8_COMPLETIONS") AS "W2L_FORM_PARTNER",
SUM("GOAL_11_COMPLETIONS") AS "CART_START",
SUM("GOAL_12_COMPLETIONS") AS "CART_COMPLETION",
SUM("TRANSACTIONS") AS "TRANSACTIONS"
*/
FROM LZ_FIVETRAN.NX_GA_INTERACTIONS.NX_GA_INTERACTIONS_BY_SOURCE ga
LEFT JOIN ANALYST_PLAYGROUND.PUBLIC.DATES d
  ON ga."DATE"::DATE = d."FULLDATE"::DATE
GROUP BY
"DATE"::date,
"SOURCE",
"MEDIUM",
"CAMPAIGN",
"LANDING_PAGE_PATH",
d."NON_ISO_WEEK_START",
d."FULLDATE")z
WHERE z."DATE" >= '2020-01-01';
