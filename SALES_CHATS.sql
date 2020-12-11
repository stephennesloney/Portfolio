CREATE OR REPLACE VIEW TF_DATA_WAREHOUSE.SALES_AND_MARKETING.SALES_CHATS
AS
SELECT
z.*,
/*Partner Page*/
CASE WHEN "PageType" IN ('Support','Help','Manage') THEN 'Non-Marketing'
  WHEN "PageType" = 'Other' THEN 'Non-Marketing'
  ELSE 'Marketing' END AS "MarketingPage",
CASE WHEN "PageType" = 'Partner' THEN 'Partner'
  ELSE 'Non-Partner' END AS "PartnerPage",
 /*Product Area*/
CASE WHEN z."Product" IN ('WordPress','WooCommerce','Magento','Flex') THEN 'Managed Apps'
  WHEN z."Product" NOT IN ('Managed WordPress','Managed WooCommerce','Magento','Flex','Unknown','Other') THEN 'Managed Hosting'
  WHEN z."Product" IN ('Unknown','Other') THEN 'Unknown'
	ELSE z."Product" END AS "ProductArea",
/*Shift by hour*/
CASE WHEN "AffiliateProgram" IN ('AWIN','CommissionJunction','ImpactRadius') THEN 'Affiliate'
	ELSE 'Non-Affiliate' END AS "AffiliateChat",
CASE WHEN "RequestHour" >= 8 AND "RequestHour" < 16 THEN 'First'
	WHEN "RequestHour" >= 16 AND "RequestHour" < 24 THEN 'Second'
	WHEN "RequestHour" >= 0 AND "RequestHour" < 8 THEN 'Third'
		ELSE 'Other' END AS "ShiftByHour"
FROM (
SELECT
date_trunc('month', LCT."REQUEST_TIME") AS "RequestStartOfMonth",
E."id" AS "EmployeeScdId",
E."name" AS "Owner Full Name",
E."department",
E."division",
/* Used to mark touch support taking sales chats on nights and weekends
CASE WHEN e."division" = 'Support' THEN 'Touch Support SBA'
	ELSE e."division" END AS "SalesTeam",
*/
1 AS "Chats",
lcb."MASTER_LABEL" as "Button Name",
LCT."ACCOUNT_ID",
LCT."AUTHENTICATION_STATUS_C",
LCT."AVERAGE_RESPONSE_TIME_OPERATOR",
LCT."AVERAGE_RESPONSE_TIME_VISITOR",
LCT."CASE_ID",
LCT."CHAT_DURATION",
LCT."CHAT_DISPOSITION_DETAILS_C",
LCT."CHAT_DISPOSITION_C",
LCT."STATUS",
LCT."OPERATOR_MESSAGE_COUNT",
LCT."BODY",
LCT."CONVERTED_TO_LEAD_C",
LCT."CONVERTED_TO_OPPORTUNITY_C",
LCT."CREATED_BY_ID",
/*Timezone Conversion*/
LCT."REQUEST_TIME",
(dateadd(hours, -5, LCT."REQUEST_TIME"))::DATE AS "RequestDate",
--EXTRACT(HOUR FROM (convert_timezone('GMT', 'EST', LCT."REQUEST_TIME"::timestamp_ntz))) AS "RequestHour",
EXTRACT(HOUR FROM (dateadd(hours, -5, LCT."REQUEST_TIME"))) AS "RequestHour",
LCT."END_TIME",
LCT."ENDED_BY",
LCT."FIRST_RESPONSE_TIME_C",
LCT."GOOGLE_PAGE_URL_C",
LCT."GOOGLE_SEND_SUCCESS_C",
LCT."ID",
LCT."IS_DELETED",
LCT."LEAD_ID",
LCT."LOCATION",
LCT."NAME",
LCT."OPPORTUNITY_C",
LCT."OWNER_ID",
split_part(LCT."LOCATION",',',3) AS "Country",
/*Checks for UTM in the URL*/
CASE WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%utm_medium%' THEN 1
		ELSE 0 END AS "HasUtmMedium",
CASE WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%utm_campaign%' THEN 1
		ELSE 0 END AS "HasUtmCampaign",
CASE WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%utm_source%' THEN 1
		ELSE 0 END AS "HasUtmSource",
CASE WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%utm_content%' THEN 1
		ELSE 0 END AS "HasUtmContent",
/*Product*/
CASE WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%cloud-dedicated%'
		THEN 'Cloud Dedicated'
	WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%/cloud-servers/%'
		THEN 'Cloud Server'
	WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%private-cloud%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%vmware%'
	THEN 'VMware'
	WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%managed-hosting-buyers-guide%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE 'https://www.liquidweb.com/blog/shared-hosting-vs-vps-vs-dedicated/%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE 'https://go.liquidweb.com/windows-hosting/%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%/products/server-clusters/%'
    	OR LCT."GOOGLE_PAGE_URL_C" LIKE '%/custom-solutions/high-availability-hosting/%'
    	OR LCT."GOOGLE_PAGE_URL_C" LIKE '%/custom-solutions/high-performance-hosting/%'
    	OR LCT."GOOGLE_PAGE_URL_C" LIKE '%/solutions/pci-compliance/%'
    	OR LCT."GOOGLE_PAGE_URL_C" LIKE '%custom%solutions%'
		/*adding pages because people dont follow processes*/
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%/black-friday-2019/%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%/year-end-2019/%'
    	OR LCT."GOOGLE_PAGE_URL_C" LIKE '%/solutions/pci-compliance/%'
	THEN 'Dedicated'
	WHEN (LCT."GOOGLE_PAGE_URL_C" LIKE '%dedi%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%hosting%happiness%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%helpful%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%advice38%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%htgod38%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%managed-hosting%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%fastwin35%')
--		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%private-cloud%'
	    --trying to remove the new VPS pages that are also on /managed-hosting/
			AND (LCT."GOOGLE_PAGE_URL_C" NOT LIKE '%vps%' AND LCT."GOOGLE_PAGE_URL_C" NOT LIKE '%woo%')
	THEN 'Dedicated'
  	WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%/products/private-cloud/%'
  	THEN 'VMware'
  	WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%/solutions/hipaa-compliant-hosting/%'
  		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%hipaa%'
  	THEN 'HIPAA'
	WHEN (LCT."GOOGLE_PAGE_URL_C" LIKE '%vps%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%htgov50%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%nowuk50%')
	AND (LCT."GOOGLE_PAGE_URL_C" NOT LIKE '%mwp%' OR LCT."GOOGLE_PAGE_URL_C" NOT LIKE '%wordpress%')
	THEN 'VPS'
	WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%cloudsites.liquidweb.com%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE 'https://go.liquidweb.com/fastcloud38/%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%cloud-sites%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%cloudsites%'
	THEN 'Cloud Sites'
	WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%woo%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%commerce%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%mwch%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE 'go.liquidweb.com/managed-hosting-for-woocommerce/%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%nexcess.net/woocommerce/%'
	THEN 'WooCommerce'
	WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%word%press%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%mwp%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%14-strategies%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%amazon-ebook%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE 'https://www.liquidweb.com/blog/liquid-web-vs-wpengine/%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%/managed-wordpress/%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%/liquid-web-vs-wpengine/%'
		OR LCT."GOOGLE_PAGE_URL_C" LIKE '%app.%liquidwebsites.com%'
    	OR LCT."GOOGLE_PAGE_URL_C" LIKE '%nexcess.net/wordpress/%'
	THEN 'WordPress'
  	WHEN "GOOGLE_PAGE_URL_C" LIKE '%nexcess.net/magento/%' 
  	THEN 'Magento'
  	WHEN "GOOGLE_PAGE_URL_C" LIKE '%nexcess.net/drupal/%'
      OR "GOOGLE_PAGE_URL_C" LIKE '%nexcess.net/bigcommerce/%'
      OR "GOOGLE_PAGE_URL_C" LIKE '%nexcess.net/sylius/%'
      OR "GOOGLE_PAGE_URL_C" LIKE '%nexcess.net/expressionengine/%'
      OR "GOOGLE_PAGE_URL_C" LIKE '%nexcess.net/craft-cms/%'
      OR "GOOGLE_PAGE_URL_C" LIKE '%nexcess.net/oro/%'
	THEN 'Flex'
  	WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%add-ons%'
    	OR LCT."GOOGLE_PAGE_URL_C" = 'Premium Business Email'
    THEN 'Add-Ons'
		ELSE 'Other' END AS "Product",
/*Company slicer using the URL*/
CASE WHEN "GOOGLE_PAGE_URL_C" ILIKE '%liquidweb.com%'
    OR "GOOGLE_PAGE_URL_C" ILIKE '%app%liquidwebsites.com%'
    OR "GOOGLE_PAGE_URL_C" ILIKE '%stormondemand.com%'
        THEN 'LW'
    WHEN "GOOGLE_PAGE_URL_C" ILIKE '%nexcess.net%'
    OR "GOOGLE_PAGE_URL_C" ILIKE '%app%.com%'
    OR "GOOGLE_PAGE_URL_C" ILIKE '%app%.net%'
        THEN 'NX'
    WHEN "GOOGLE_PAGE_URL_C" IS NULL THEN 'NULL'
    ELSE 'Other'
        END AS "COMPANY",		
/*PageType*/
CASE WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%com/products/%'
    OR LCT."GOOGLE_PAGE_URL_C" LIKE '%com/products-catalog/%'
    OR LCT."GOOGLE_PAGE_URL_C" LIKE '%com/Dedicated/%' THEN 'Product'
  WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%com/kb/%' THEN 'KB'
  WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%/blog/%' THEN 'Blog'
  WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%www.liquidweb.com/'
	AND LCT."GOOGLE_PAGE_URL_C" NOT LIKE '%www.liquidweb.com/?s=%' THEN 'HomePage'
  WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%/blog/%' THEN 'Blog'
  WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%help.liquidweb.com%' THEN 'Help'
  WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%hub.liquidweb.com/%' THEN 'Hub'
  WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%cart.liquidweb.com%' THEN 'Cart'
  WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%manage.liquidweb.com%' THEN 'Manage'
  WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%go.liquidweb.com/%' THEN 'Go'
  WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%com/support/%' THEN 'Support'
  WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%com/solutions/%'
    AND LCT."GOOGLE_PAGE_URL_C" NOT LIKE '%solutions/reseller%' THEN 'Solutions'
  WHEN (LCT."GOOGLE_PAGE_URL_C" LIKE '%partner%'
    OR LCT."GOOGLE_PAGE_URL_C" LIKE '%reseller%'
    OR LCT."GOOGLE_PAGE_URL_C" LIKE '%affiliate%'
    OR LCT."GOOGLE_PAGE_URL_C" LIKE 'https://go.liquidweb.com/web-professional-program-creators/%')
    AND LCT."GOOGLE_PAGE_URL_C" NOT LIKE '%/s/case/%' THEN 'Partner'
	ELSE 'Other' END AS "PageType",
CASE WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%partner%' THEN 'Partner'
  WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%reseller%' THEN 'Reseller'
  WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%affiliate%' THEN 'Affiliate'
  WHEN LCT."GOOGLE_PAGE_URL_C" LIKE 'https://go.liquidweb.com/web-professional-program-creators/%'
    AND LCT."GOOGLE_PAGE_URL_C" NOT LIKE '%/s/case/%' THEN 'Creators'
	ELSE 'Other' END AS "PageTypeAffiliate",
CASE WHEN COALESCE(LCT."CHAT_DISPOSITION_C",'') IN ('Abuse','Duplicate','No Customer Present at Start','Spam or Junk Lead','Spam/Abuse')
  THEN 'Unqualified'
	ELSE 'Qualified' END AS "IsSalesQualified",
CASE WHEN COALESCE(LCT."CHAT_DISPOSITION_C",'') NOT IN ('Abuse','Duplicate','No Customer Present at Start','Spam or Junk Lead','Spam/Abuse')
  THEN 1 ELSE 0 END AS "QualifiedSalesChats",
CASE WHEN lcb."MASTER_LABEL" IN ('Sales Chat Invite','WordPress Invite','CloudSites Invitation') THEN 'Yes'
    ELSE 'No' END AS "ProactiveChat",
A."ID" AS "ChatAccountId",
L."ID" AS "ChatLeadId",
L."CONVERTED_OPPORTUNITY_ID",
o."ID" AS "ChatOpportunityId",
L."LOGO_TYPE_C" AS "LeadLogoType",
o."TYPE" AS "OpportunityType",
A."ACCOUNT_STATUS_C",
CASE WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%&_aw=1%' THEN 'AWIN'
  WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%&_cj=1%' THEN 'CommissionJunction'
  WHEN LCT."GOOGLE_PAGE_URL_C" LIKE '%&_ir=1%' THEN 'ImpactRadius'
  ELSE 'Non-Affiliate' END AS "AffiliateProgram",
CASE WHEN o."TYPE" IS NULL THEN 0
	ELSE 1 END AS "HasOppType",
CASE WHEN L."LOGO_TYPE_C" IS NULL THEN 0
	ELSE 1 END AS "HasLeadType",
CASE WHEN A."ID" IS NULL THEN 0
	ELSE 1 END AS "HasAccount",
CASE WHEN LCT."AUTHENTICATION_STATUS_C" = 'Authenticated' THEN 1
	ELSE 0 END AS "AuthenticatedUser",
CASE WHEN c."ID" IS NULL THEN 0
	ELSE 1 END AS "HasContact",
CASE WHEN LCT."AUTHENTICATION_STATUS_C" = 'Authenticated'
  OR COALESCE(CA."AUTHENTICATION_STATUS_C",'') = 'Authenticated'
  OR COALESCE(o."TYPE",'') LIKE '%Existing%'
  OR COALESCE(LCT."CHAT_DISPOSITION_C",'') = 'Existing Client' THEN 'Existing'
	ELSE 'New' END AS "NewOrExisting",
CA."CASE_NUMBER",
CA."AUTHENTICATION_STATUS_C" AS "CaseAuthenticationStatus",
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
D."NON_ISO_WEEK_END" AS "WeekEnd",
/*UTM Parameters*/
SPLIT_PART(SPLIT_PART(COALESCE(LCT."GOOGLE_PAGE_URL_C",''), 'utm_campaign=', 2),'&',1) AS "UTM_Campaign",
SPLIT_PART(SPLIT_PART(COALESCE(LCT."GOOGLE_PAGE_URL_C",''), 'utm_medium=', 2),'&',1) AS "UTM_Medium",
SPLIT_PART(SPLIT_PART(COALESCE(LCT."GOOGLE_PAGE_URL_C",''), 'utm_source=', 2),'&',1) AS "UTM_Source",
SPLIT_PART(SPLIT_PART(COALESCE(LCT."GOOGLE_PAGE_URL_C",''), 'utm_content=', 2),'&',1) AS "UTM_Content"

FROM "LZ_FIVETRAN"."SALESFORCE"."LIVE_CHAT_TRANSCRIPT" LCT
JOIN "LZ_FIVETRAN"."SALESFORCE"."LIVE_CHAT_BUTTON" LCB
	ON LCB."ID" = LCT."LIVE_CHAT_BUTTON_ID"
LEFT JOIN "LZ_PRODUCTION_ANALYSIS"."scd"."employee_scd" E
  ON LCT."OWNER_ID" = E."user_id"
  AND E."start_dtg" < LCT."REQUEST_TIME"
  AND E."end_dtg" >= LCT."REQUEST_TIME"
LEFT JOIN "LZ_FIVETRAN"."SALESFORCE"."ACCOUNT" A
	ON A."ID" = LCT."ACCOUNT_ID"
LEFT JOIN "LZ_FIVETRAN"."SALESFORCE"."LEAD" L
  ON L."ID" = LCT."LEAD_ID"
LEFT JOIN "LZ_FIVETRAN"."SALESFORCE"."OPPORTUNITY" O
  ON O."ID" = LCT."OPPORTUNITY_C"
LEFT JOIN "LZ_FIVETRAN"."SALESFORCE"."CONTACT" C
  ON C."ID" = LCT."CONTACT_ID"
LEFT JOIN "LZ_FIVETRAN"."SALESFORCE"."CASE" CA
  ON LCT."CASE_ID" = CA."ID"
LEFT JOIN "ANALYST_PLAYGROUND"."PUBLIC"."DATES" D
  ON (dateadd(hours, -5, LCT."REQUEST_TIME"))::DATE = D."FULLDATE"
WHERE LCT."SALES_CHAT_C" = TRUE
AND LCT."STATUS" = 'Completed')Z
ORDER BY "REQUEST_TIME" DESC;
