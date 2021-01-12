--_______________________________________________________________________________________________________________________________________________________________________________
--Part 1

-- In part 1 I'll lay out some various subgroups I created and hopefully use comments to explain as I go about my thinking. 

--since we know we need the loss ratio, I will calulcate that at a policy level so we can then aggregate and average out loss ratios per sub group. 
--to have a numerator and denomenator for the loss ratio we must first get that lost ratio for each policy, then we can determine what the features are important and what policies fall into each of those features sets. 
--this could be solved by cartesian matrix, but that's easier for me to do outside of SQL (which isn't allowed) so I'll go with self-defined sub groups

--this first bit is just a select * from the nested unions, orders it by DESC loss ratio, and adds the row number for rank of worst loss ratio
SELECT 
*,
ROW_NUMBER() OVER(ORDER BY z."loss_ratio" DESC) AS "rank_of_loss_ratio"
FROM (
	--inside the first nest I have several CTEs that calculate various things
	--the first CTE is called loss_ratios, this is where we calculate the per policy loss ratio values
	With loss_ratios AS (
		WITH numerator AS (
			SELECT 
			"policy_id",
			sum(cp."claims_paid_amount") AS "amount_paid_out"
			FROM onwh.claim_payout cp
			GROUP BY 1
		),
		denomenator AS (
			SELECT 
			"policy_id",
			sum(p."premium_amount") AS "amount_paid_in"
			FROM moola.payments p 
			GROUP BY 1
		)
		SELECT
		--take the policy ID from vehicles table, I don't have a great idea of the tables but you would want the table with every policy on it
		"policy_id",
		--this column is a nested calculation that determined the claims total per policy and divides it by the total for the payments made
		(n."amount_paid_out" / d."amount_paid_in") AS "loss_ratio",
		--this adds a column with just 1s in it so we can sum for a policy count
		1 AS "policy_count"
		FROM best_pol.vehicles v
		LEFT JOIN numerator n
			ON v."policy_id" = n."policy_id"
		LEFT JOIN denomenator d
			ON v."policy_id" = d."policy_id"
		--with this I now have a loss ratio and policy count per policy ID which I can then call into the subgroups necessary
		--note that one policy can be in multiple subgroups this way
	)
	--now that each account has a ratio associated we can use the AVG aggregate and summing the policy count column to get the average loss ratio and count for each type of sub_group I define. 
	--after the groups are defined I will create a CTE that determines a value for each one and then union them all together 
	
	--this CTE creates a subgroup looking at cars with alarms to see if that was a factor in increased claims
	--these sub groups are arbitrarily defined at the moment, but could be defined as whatever groups we want to check
	WITH has_alarm AS (
	SELECT
	'has_alarm' AS "sub_group"
	AVG(loss_ratios."loss_ratio") AS "loss_ratio",
	SUM(loss_ratios."policy_county") AS "policy_count"
	FROM best_pol.vehicles v
	LEFT JOIN loss_ratios lr
		ON v.policy_id = lr.policy_id
	HAVING v."has_alarm" = 1
	),
	--this is testing the opposite, cars without alarms
	no_alarm AS (
	SELECT
	'no_alarm' AS "sub_group",
	AVG(loss_ratios."loss_ratio") AS "loss_ratio",
	SUM(loss_ratios."policy_county") AS "policy_count"
	FROM best_pol.vehicles v
	LEFT JOIN loss_ratios lr
		ON v.policy_id = lr.policy_id
	HAVING v."has_alarm" = 0
	),
	--this is just checking if customers we've previously insured are any more likely to make claims
	previously_insured AS (
	SELECT
	'previously_insured' AS "sub_group",
	AVG(loss_ratios."loss_ratio") AS "loss_ratio",
	SUM(loss_ratios."policy_county") AS "policy_count"
	FROM prev_pol.policy_features p
	LEFT JOIN loss_ratios lr
		ON p.policy_id = lr.policy_id
	--this having clause could have also come from the best_pol.policies table which has the features as well
	HAVING p."feature_name" = 'was_previously_insured'
	AND p."feature_value" = '1'
	),
	--this is just checking if customers we have never insures are more likely to make claims
	not_previously_insured AS (
	SELECT
	'not_previously_insured' AS "sub_group",
	AVG(loss_ratios."loss_ratio") AS "loss_ratio",
	SUM(loss_ratios."policy_county") AS "policy_count"
	FROM prev_pol.policy_features p
	LEFT JOIN loss_ratios lr
		ON p.policy_id = lr.policy_id
	HAVING p."feature_name" = 'was_previously_insured'
	AND p."feature_value" = '0'
	),
	--I would expect this section to be very highly correlated with the actual results if your AI is good at its job. This could also be used to determine if the AI is successful. If we see risk level C as the best loss ratio we know something is strange. 
	risk_profile_a AS (
	SELECT
	'risk_profile_a' AS "sub_group",
	AVG(loss_ratios."loss_ratio") AS "loss_ratio",
	SUM(loss_ratios."policy_county") AS "policy_count"
	FROM prev_pol.policy_features p
	LEFT JOIN loss_ratios lr
		ON p.policy_id = lr.policy_id
	HAVING p."feature_name" = 'risk_profile'
	AND p."feature_value" = 'A'
	),
	--same as risk level a
	risk_profile_b AS (
	SELECT
	'risk_profile_b' AS "sub_group",
	AVG(loss_ratios."loss_ratio") AS "loss_ratio",
	SUM(loss_ratios."policy_county") AS "policy_count"
	FROM prev_pol.policy_features p
	LEFT JOIN loss_ratios lr
		ON p.policy_id = lr.policy_id
	HAVING p."feature_name" = 'risk_profile'
	AND p."feature_value" = 'B'
	),
	--same as risk level a and b
	risk_profile_c AS (
	SELECT
	'risk_profile_c' AS "sub_group",
	AVG(loss_ratios."loss_ratio") AS "loss_ratio",
	SUM(loss_ratios."policy_county") AS "policy_count"
	FROM prev_pol.policy_features p
	LEFT JOIN loss_ratios lr
		ON p.policy_id = lr.policy_id
	HAVING p."feature_name" = 'risk_profile'
	AND p."feature_value" = 'C'
	),
	--this is to see if maybe more affordable cars could correlate to poorer areas that have more claims and vice versa
	vehicle_value_over_10k AS (
	SELECT
	'vehicle_value_over_10k' AS "sub_group",
	AVG(loss_ratios."loss_ratio") AS "loss_ratio",
	SUM(loss_ratios."policy_county") AS "policy_count"
	FROM best_pol.vehicles v
	LEFT JOIN loss_ratios lr
		ON v.policy_id = lr.policy_id
	HAVING v."vehicle_value" >= 10000
	), 
	--same as value over 10k 
	vehicle_value_under_10k AS (
	SELECT
	'vehicle_value_under_10k' AS "sub_group",
	AVG(loss_ratios."loss_ratio") AS "loss_ratio",
	SUM(loss_ratios."policy_county") AS "policy_count"
	FROM best_pol.vehicles v
	LEFT JOIN loss_ratios lr
		ON v.policy_id = lr.policy_id
	HAVING v."vehicle_value" < 10000
	),
	--this checks for cars that barely move all year
	mileage_under_1k AS (
	SELECT
	'mileage_under_1k' AS "sub_group",
	AVG(loss_ratios."loss_ratio") AS "loss_ratio",
	SUM(loss_ratios."policy_county") AS "policy_count"
	FROM best_pol.vehicles v
	LEFT JOIN loss_ratios lr
		ON v.policy_id = lr.policy_id
	HAVING v."mileage" < 1000
	),
	--this checks for cars that move over 1k miles per year
	mileage_over_1k AS (
	SELECT
	'mileage_under_1k' AS "sub_group",
	AVG(loss_ratios."loss_ratio") AS "loss_ratio",
	SUM(loss_ratios."policy_county") AS "policy_count"
	FROM best_pol.vehicles v
	LEFT JOIN loss_ratios lr
		ON v.policy_id = lr.policy_id
	HAVING v."mileage" > 1000
	),
	--this checks cars with higher mileage in comparison to the previous 2 groups. If long miles are associated with more claims we should see a pretty distinct difference between these groups. 
	--I believe pre-covid the average mileage was something like 14k. I just picked 10k as an arbitrary number looking at the values in the table
	mileage_over_10k AS (
	SELECT
	'mileage_under_1k' AS "sub_group",
	AVG(loss_ratios."loss_ratio") AS "loss_ratio",
	SUM(loss_ratios."policy_county") AS "policy_count"
	FROM best_pol.vehicles v
	LEFT JOIN loss_ratios lr
		ON v.policy_id = lr.policy_id
	HAVING v."mileage" > 10000
	)
	--one disadvantage of doing it this way is that it requires us to create custom CTEs for each subgroup we want to check, the advantage is that you can rank any custom requirements you can think up against everything else. 
	--so now that we have all these separate tables with the same 3 columns (sub_group, loss_ratio, and policy_count) we can union them all together so they are individual rows in the same table instead
	SELECT * FROM has_alarm
	UNION 
	SELECT * FROM no_alarm
	UNION 
	SELECT * FROM previously_insured
	UNION 
	SELECT * FROM not_previously_insured
	UNION 
	SELECT * FROM risk_profile_a
	UNION 
	SELECT * FROM risk_profile_b
	UNION 
	SELECT * FROM risk_profile_c
	UNION 
	SELECT * FROM vehicle_value_over_10k
	UNION 
	SELECT * FROM vehicle_value_under_10k
	UNION 
	SELECT * FROM mileage_under_1k
	UNION 
	SELECT * FROM mileage_over_1k
	UNION 
	SELECT * FROM mileage_over_10k
) z
--this orders sub_groups by the worst loss ratios, DESC so the highest (bad) ratio is on top
ORDER BY z."loss_ratio" DESC;

--IN CONCLUSION
--the nice thing about this is that policies can be in multiple groups, so you can see if no_alarm and risk_assessment_c are #1 and #2 you could very likely infer that a policy that fell into both of these groups should be very heavily scrutinized. Possibly even take that info and work it into the AI bot so that the risk assessment in the first place already. 




--___________________________________________________________________________________________________________________________________________________________________________________________________________________
--Part 2

--I had some extra time at the end (only took 1.5 hours to get this far) so I was just looking for additional things that could be relevant. 
--I believe location data is one important factor for insurance AI which is usually very local. Only location data listed here is the state pulled from the policy number (assuming the first 2 characters of the policy number are the state) 

--this section could be added to the CTE section below the loss_ratios CTE
WITH state_rank AS(
	SELECT 
	y.*,
	ROW_NUMBER() OVER(ORDER BY loss_ratios."loss_ratio" DESC) AS "rank_of_loss_ratio"
	FROM (
		SELECT 
		LEFT(Z."policy_id",2) AS "policy_state"
		AVG(loss_ratios."loss_ratio") AS "loss_ratio",
		SUM(loss_ratios."policy_county") AS "policy_count"
		FROM best_pol.vehicles v
		LEFT JOIN loss_ratios lr
			ON v.policy_id = lr.policy_id
		GROUP BY LEFT(Z."policy_id",2)
		) y
)

--this will give you a list of the worst states by loss ratio, this could be done as individual CTEs in the above logic but I did this separate so I could learn which states are the worst since we have to create individual CTEs for them I thought it would be silly to do all 50 but perhaps we could add in the top 5. 

--example of CTEs that would could pull in dynamic states values from this state_rank calculated above for the 3 worst states
	worst_state AS (
	SELECT
	'worst_state' AS "sub_group",
	AVG(loss_ratios."loss_ratio") AS "loss_ratio",
	SUM(loss_ratios."policy_county") AS "policy_count"
	FROM best_pol.vehicles v
	LEFT JOIN loss_ratios lr
		ON v.policy_id = lr.policy_id
	HAVING state_rank."rank_of_loss_ratio" = 1
	),
	2nd_worst_state AS (
	SELECT
	'worst_state' AS "sub_group",
	AVG(loss_ratios."loss_ratio") AS "loss_ratio",
	SUM(loss_ratios."policy_county") AS "policy_count"
	FROM best_pol.vehicles v
	LEFT JOIN loss_ratios lr
		ON v.policy_id = lr.policy_id
	HAVING state_rank."rank_of_loss_ratio" = 2
	),
	3rd_worst_state AS (
	SELECT
	'worst_state' AS "sub_group",
	AVG(loss_ratios."loss_ratio") AS "loss_ratio",
	SUM(loss_ratios."policy_county") AS "policy_count"
	FROM best_pol.vehicles v
	LEFT JOIN loss_ratios lr
		ON v.policy_id = lr.policy_id
	HAVING state_rank."rank_of_loss_ratio" = 3
	)

--add this section into the unions as well
	SELECT * FROM mileage_under_1k
	UNION 
	SELECT * FROM mileage_over_1k
	UNION 
	SELECT * FROM mileage_over_10k




--	IN CONCLUSION
-- Hope this was easy enough to follow. I'm happy to schedule time to walk through the results. 
