-- query for dagger pipeline

WITH test_info AS
(
SELECT
  1101 AS ab_test_id,
  2457 AS control,
  2519 AS experiment,  
),

uncapped_downfunnel_per_auction AS
	(SELECT
	    COALESCE(install__ad_click__impression__auction_id,
	             reeng_click__impression__auction_id,
	             attribution_event__click__impression__auction_id) AS auction_id
	  , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(COALESCE(install__ad_click__impression__at, reeng_click__impression__at, attribution_event__click__impression__at)/1000, 'UTC'))),1,19),'Z') as impression_at
	  , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(COALESCE(install__ad_click__at, reeng_click__at, attribution_event__click__at)/1000, 'UTC'))),1,19),'Z') as click_at
	  , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(install__at/1000, 'UTC'))),1,19),'Z') AS install_at
	  , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS at  
	  , "group".id AS ab_test_group_id
	  , "group".name AS ab_test_group_name
	  , COALESCE(install__ad_click__impression__bid__bid_request__exchange,
	             reeng_click__impression__bid__bid_request__exchange,
	             attribution_event__click__impression__bid__bid_request__exchange) AS exchange
	  , COALESCE(install__ad_click__impression__bid__customer_id,
	             reeng_click__impression__bid__customer_id,
	             attribution_event__click__impression__bid__customer_id) AS customer_id
	  , COALESCE(install__ad_click__impression__bid__app_id,
	             reeng_click__impression__bid__app_id,
	             attribution_event__click__impression__bid__app_id) AS dest_app_id
	  , COALESCE(install__ad_click__impression__bid__campaign_id,
	             reeng_click__impression__bid__campaign_id,
	             attribution_event__click__impression__bid__campaign_id) AS campaign_id
	  , CASE WHEN COALESCE(install__ad_click__impression__bid__bid_request__device__geo__country,
	             reeng_click__impression__bid__bid_request__device__geo__country,
	             attribution_event__click__impression__bid__bid_request__device__geo__country) IN 
	  			('ZA','KE','AE','SA','BH','KW','OM','QA','LB','JO','SY','IQ','IR','IL','PS','CY','YE',
	  				'EG','TR','AZ','AM','GE','KZ','UZ','TM','AF','PK','KG','TJ','SG','ID','MY','KR','TW','HK','PH','TH','VN') THEN 'asia pacific'
	  		 WHEN COALESCE(install__ad_click__impression__bid__bid_request__device__geo__country,
	             reeng_click__impression__bid__bid_request__device__geo__country,
	             attribution_event__click__impression__bid__bid_request__device__geo__country) IN ('US','GB','IN','JP','BR') THEN COALESCE(install__ad_click__impression__bid__bid_request__device__geo__country,
																														             reeng_click__impression__bid__bid_request__device__geo__country,
																														             attribution_event__click__impression__bid__bid_request__device__geo__country)
	         ELSE 'others' END AS country_grouped
	  , CAST(COALESCE(install__ad_click__impression__bid__bid_request__non_personalized,
	             reeng_click__impression__bid__bid_request__non_personalized,
	             attribution_event__click__impression__bid__bid_request__non_personalized) AS varchar) AS non_personalized
	  , CAST(is_viewthrough AS varchar) AS is_viewthrough
	  , COALESCE(install__ad_click__impression__bid__app_platform,
	             reeng_click__impression__bid__app_platform,
	             attribution_event__click__impression__bid__app_platform) AS platform
	  , COALESCE(install__ad_click__impression__bid__ad_group_type,
	             reeng_click__impression__bid__ad_group_type,
	             attribution_event__click__impression__bid__ad_group_type) AS ad_group_type
	  , COALESCE(install__ad_click__impression__bid__price_data__model_type,
	             reeng_click__impression__bid__price_data__model_type,
	             attribution_event__click__impression__bid__price_data__model_type) AS model_type
	  , COALESCE(install__ad_click__impression__bid__exploratory, 
	             reeng_click__impression__bid__exploratory,
	             attribution_event__click__impression__bid__exploratory) AS is_exploratory  
	  , CASE WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = 'video' then 'VAST'
	            WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = 'native' then 'native'
	            WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) in ('320x50', '728x90') then 'banner'
	            WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = '300x250' then 'mrec'
	            ELSE 'html-interstitial' END AS ad_format
	  , COALESCE(install__ad_click__impression__bid__campaign_tracker_type,
	             reeng_click__impression__bid__campaign_tracker_type,
	             attribution_event__click__impression__bid__campaign_tracker_type) AS campaign_tracker_type
	  , COALESCE(install__ad_click__impression__bid__bid_request__auction_type,
	             reeng_click__impression__bid__bid_request__auction_type,
	             attribution_event__click__impression__bid__bid_request__auction_type) AS auction_type
	  , CASE
	        WHEN contains(COALESCE(install__ad_click__impression__bid__auction_result__winner__spend_group__ab_test_group_ids,
	              reeng_click__impression__bid__auction_result__winner__spend_group__ab_test_group_ids,
	              attribution_event__click__impression__bid__auction_result__winner__spend_group__ab_test_group_ids), 2457) THEN 'control'
	        WHEN contains(COALESCE(install__ad_click__impression__bid__auction_result__winner__spend_group__ab_test_group_ids,
	              reeng_click__impression__bid__auction_result__winner__spend_group__ab_test_group_ids,
	              attribution_event__click__impression__bid__auction_result__winner__spend_group__ab_test_group_ids), 2519) THEN 'experiment' 
	        ELSE 'none' END AS spend_based_ab_test_group
	  , SUM(0) AS impressions
	  , SUM(0) AS spend_micros
	  , SUM(0) AS revenue_micros
	  , SUM(0) AS clicks
	  , SUM(0) AS installs
	  , SUM(if(custom_event_id = COALESCE(install__ad_click__impression__bid__campaign_target_event_id,reeng_click__impression__bid__campaign_target_event_id,attribution_event__click__impression__bid__campaign_target_event_id)
	  		AND at - COALESCE(install__ad_click__impression__at, reeng_click__impression__at, attribution_event__click__impression__at) < 604800000	
	  		,1,0)) AS target_events_d7
	  , SUM(if(at - COALESCE(install__ad_click__impression__at, reeng_click__impression__at, attribution_event__click__impression__at) < 604800000
	  		,customer_revenue_micros, 0)) AS customer_revenue_micros _d7
	  , SUM(if(custom_event_id = COALESCE(install__ad_click__impression__bid__campaign_target_event_id,reeng_click__impression__bid__campaign_target_event_id,attribution_event__click__impression__bid__campaign_target_event_id)
	        AND first_occurrence
	        AND at - COALESCE(install__ad_click__impression__at, reeng_click__impression__at, attribution_event__click__impression__at) < 604800000
	        ,1,0)) AS target_events_first_d7
	  , SUM(0) AS expected_val
	  , SUM(0) AS bids
	  , SUM(0) AS aovx_nr_micros
	FROM rtb.matched_app_events ae
	CROSS JOIN UNNEST(COALESCE(
	          install__ad_click__impression__bid__bid_request__ab_test_assignments,
	          reeng_click__impression__bid__bid_request__ab_test_assignments)) t
	WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
	  AND is_uncredited <> true
	  AND for_reporting = true
	  AND t.id = (SELECT ab_test_id FROM test_info)
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
	),

funnel AS (
	SELECT
	   NULL AS impression_at
	  , NULL AS click_at 
	  , NULL AS install_at
	  , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS at
	  , "group".id AS ab_test_group_id
	  , "group".name AS ab_test_group_name
	  , bid_request__exchange AS exchange
	  , customer_id AS customer_id
	  , app_id AS dest_app_id
	  , campaign_id AS campaign_id
	  , CASE WHEN bid_request__device__geo__country) IN 
	  			('ZA','KE','AE','SA','BH','KW','OM','QA','LB','JO','SY','IQ','IR','IL','PS','CY','YE',
	  				'EG','TR','AZ','AM','GE','KZ','UZ','TM','AF','PK','KG','TJ','SG','ID','MY','KR','TW','HK','PH','TH','VN') THEN 'asia pacific'
	  		 WHEN bid_request__device__geo__country IN ('US','GB','IN','JP','BR') THEN bid_request__device__geo__country
	         ELSE 'others' END AS country_grouped
	  , cast(bid_request__non_personalized AS varchar) AS non_personalized
	  , 'N/A' AS is_viewthrough
	  , app_platform AS platform
	  , ad_group_type AS ad_group_type
	  , price_data__model_type AS model_type
	  , exploratory AS is_exploratory
	  , CASE WHEN creative__ad_format = 'video' THEN 'VAST'
	        WHEN creative__ad_format = 'native' THEN 'native'
	        WHEN creative__ad_format IN ('320x50', '728x90') THEN 'banner'
	        WHEN creative__ad_format = '300x250' THEN 'mrec'
	        ELSE 'html-interstitial' END AS ad_format
	  , campaign_tracker_type AS campaign_tracker_type
	  , bid_request__auction_type AS auction_type
	  , CASE WHEN contains(auction_result__winner__spend_group__ab_test_group_ids, 2457) THEN 'control'
	        WHEN contains(auction_result__winner__spend_group__ab_test_group_ids, 2519) THEN 'experiment'
	        ELSE 'none' END AS spend_based_ab_test_group	 
	  , SUM(0) AS impressions
	  , SUM(0) AS spend_micros
	  , SUM(0) AS revenue_micros
	  , SUM(0) AS installs
	  , SUM(0) AS target_events_d7
	  , SUM(0) AS customer_revenue_micros_d7
	  , SUM(0) AS target_events_first_d7
	  , SUM(0) AS capped_customer_revenue_micros_d7
	  , SUM(0) AS squared_capped_customer_revenue_d7
	  , SUM(0) AS expected_val
	  , SUM(1/sample_rate) AS bids
	  , SUM(0) AS aovx_nr_micros
	FROM rtb.market_price_bids m
	CROSS JOIN UNNEST(bid_request__ab_test_assignments) t	
	WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
	AND t.id = (SELECT ab_test_id FROM test_info)
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21

UNION ALL 

	SELECT
	    CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS impression_at
	  , NULL AS click_at
	  , NULL AS install_at
	  , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS at
	  , "group".id AS ab_test_group_id
	  , "group".name AS ab_test_group_name
	  , bid__bid_request__exchange AS exchange
	  , bid__customer_id AS customer_id
	  , bid__app_id AS dest_app_id
	  , bid__campaign_id AS campaign_id
	  , CASE WHEN bid__bid_request__device__geo__country) IN 
	  			('ZA','KE','AE','SA','BH','KW','OM','QA','LB','JO','SY','IQ','IR','IL','PS','CY','YE',
	  				'EG','TR','AZ','AM','GE','KZ','UZ','TM','AF','PK','KG','TJ','SG','ID','MY','KR','TW','HK','PH','TH','VN') THEN 'asia pacific'
	  		 WHEN bid__bid_request__device__geo__country IN ('US','GB','IN','JP','BR') THEN bid__bid_request__device__geo__country
	         ELSE 'others' END AS country_grouped 
	  , CAST(bid__bid_request__non_personalized AS varchar) AS non_personalized
	  , 'N/A' AS is_viewthrough
	  , bid__app_platform AS platform
	  , bid__ad_group_type AS ad_group_type
	  , bid__price_data__model_type AS model_type
	  , bid__exploratory AS is_exploratory
	  , bid__bid_request__impressions[1].traffic_type AS traffic_type
	  , CASE WHEN bid__creative__ad_format = 'video' THEN 'VAST'
	        WHEN bid__creative__ad_format = 'native' THEN 'native'
	        WHEN bid__creative__ad_format in ('320x50', '728x90') THEN 'banner'
	        WHEN bid__creative__ad_format = '300x250' THEN 'mrec'
	        ELSE 'html-interstitial' END AS ad_format
	  , bid__campaign_tracker_type AS campaign_tracker_type
	  , bid__bid_request__auction_type AS auction_type
	  , CASE
	        WHEN contains(bid__auction_result__winner__spend_group__ab_test_group_ids, 2457) THEN 'control'
	        WHEN contains(bid__auction_result__winner__spend_group__ab_test_group_ids, 2519) THEN 'experiment'
	        ELSE 'none' END AS spend_based_ab_test_group	  
	  , COUNT(*) AS impressions
	  , SUM(spend_micros) AS spend_micros
	  , SUM(revenue_micros) AS revenue_micros
	  , SUM(0) AS installs
	  , SUM(0) AS target_events_d7
	  , SUM(0) AS customer_revenue_micros_d7
	  , SUM(0) AS target_events_first_d7
	  , SUM(0) AS capped_customer_revenue_micros_d7
	  , SUM(0) AS squared_capped_customer_revenue_d7
	  , SUM(bid__price_data__conversion_likelihood) AS expected_val
	  , SUM(0) AS bids
	  , SUM(CASE
	        WHEN bid__bid_request__impressions[1].vungle_revenue.payout_type = 'FLAT_CPM' 
	             AND bid__bid_request__exchange = 'VUNGLE'
	        THEN (spend_micros - (bid__bid_request__impressions[1].vungle_revenue.publisher_flat_cpm_micros/1000))
	        WHEN bid__bid_request__impressions[1].vungle_revenue.payout_type = 'REVENUE_SHARE' 
	             AND bid__bid_request__exchange = 'VUNGLE'
	        THEN spend_micros * (1.0 - bid__bid_request__impressions[1].vungle_revenue.publisher_revenue_share)
	        WHEN bid__bid_request__impressions[1].vungle_revenue.payout_type = 'HEADER_BIDDING'
	             AND bid__bid_request__exchange = 'VUNGLE'
	        THEN spend_micros - mediation_price_micros
	        ELSE 0 END) AS aovx_nr_micros
	FROM rtb.impressions_with_bids i
	CROSS JOIN UNNEST(bid__bid_request__ab_test_assignments) t
	WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
	   AND t.id = (SELECT ab_test_id FROM test_info)
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21

UNION ALL 

	SELECT
	    CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(ad_click__impression__at/1000, 'UTC'))),1,19),'Z') as impression_at
	  , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(ad_click__at/1000, 'UTC'))),1,19),'Z') as click_at
	  , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') as install_at
	  , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS at  
	  , "group".id AS ab_test_group_id
	  , "group".name AS ab_test_group_name
	  , ad_click__impression__bid__bid_request__exchange AS exchange
	  , ad_click__impression__bid__customer_id AS customer_id
	  , ad_click__impression__bid__app_id AS dest_app_id
	  , ad_click__impression__bid__campaign_id AS campaign_id
	  , CASE WHEN ad_click__impression__bid__bid_request__device__geo__country) IN 
	  			('ZA','KE','AE','SA','BH','KW','OM','QA','LB','JO','SY','IQ','IR','IL','PS','CY','YE',
	  				'EG','TR','AZ','AM','GE','KZ','UZ','TM','AF','PK','KG','TJ','SG','ID','MY','KR','TW','HK','PH','TH','VN') THEN 'asia pacific'
	  		 WHEN ad_click__impression__bid__bid_request__device__geo__country IN ('US','GB','IN','JP','BR') THEN ad_click__impression__bid__bid_request__device__geo__country
	         ELSE 'others' END AS country_grouped 
	  , cast(ad_click__impression__bid__bid_request__non_personalized as varchar) as non_personalized
	  , CAST(is_viewthrough AS VARCHAR) AS is_viewthrough
	  , ad_click__impression__bid__app_platform AS platform
	  , ad_click__impression__bid__ad_group_type AS ad_group_type
	  , ad_click__impression__bid__price_data__model_type AS model_type
	  , ad_click__impression__bid__exploratory AS is_exploratory
	  , CASE WHEN ad_click__impression__bid__creative__ad_format = 'video' THEN 'VAST'
	        WHEN ad_click__impression__bid__creative__ad_format = 'native' THEN 'native'
	        WHEN ad_click__impression__bid__creative__ad_format in ('320x50', '728x90') THEN 'banner'
	        WHEN ad_click__impression__bid__creative__ad_format = '300x250' THEN 'mrec'
	        ELSE 'html-interstitial' END AS ad_format
	  , ad_click__impression__bid__campaign_tracker_type AS campaign_tracker_type
	  , ad_click__impression__bid__bid_request__auction_type AS auction_type
	  , CASE
	        WHEN contains(ad_click__impression__bid__auction_result__winner__spend_group__ab_test_group_ids, 2457) THEN 'control'
	        WHEN contains(ad_click__impression__bid__auction_result__winner__spend_group__ab_test_group_ids, 2519) THEN 'win-and-imp-02-98-rescaled'
	        ELSE 'none' END AS spend_based_ab_test_group
	  , SUM(0) AS impressions
	  , SUM(0) AS spend_micros
	  , SUM(0) AS revenue_micros
	  , COUNT(*) AS installs
	  , SUM(0) AS target_events_d7
	  , SUM(0) AS customer_revenue_micros_d7
	  , SUM(0) AS target_events_first_d7
	  , SUM(0) AS capped_customer_revenue_micros_d7
	  , SUM(0) AS squared_capped_customer_revenue_d7
	  , SUM(0) AS expected_val
	  , SUM(0) AS bids
	  , SUM(0) AS aovx_nr_micros
	FROM rtb.matched_installs mi
	CROSS JOIN UNNEST(ad_click__impression__bid__bid_request__ab_test_assignments) t
	WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
	  AND is_uncredited <> true
	  AND for_reporting = true
	  AND t.id = (SELECT ab_test_id FROM test_info)
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21

UNION ALL 

	select
	    impression_at
	  , click_at
	  , install_at
	  , at  
	  , ab_test_group_id
	  , ab_test_group_name
	  , exchange
	  , customer_id
	  , dest_app_id
	  , campaign_id
	  , country_grouped
	  , non_personalized
	  , is_viewthrough
	  , platform
	  , ad_group_type
	  , model_type
	  , is_exploratory
	  , ad_format
	  , campaign_tracker_type
	  , auction_type
	  , spend_based_ab_test_group
	  , SUM(0) AS impressions
	  , SUM(0) AS spend_micros
	  , SUM(0) AS revenue_micros
	  , SUM(0) AS installs
	  , SUM(target_events_d7) AS target_events_d7
	  , SUM(customer_revenue_micros_d7) AS customer_revenue_micros_d7 
	  , SUM(target_events_first_d7) AS target_events_first_d7
	  , SUM(LEAST(customer_revenue_micros_d7,500000000)) AS capped_customer_revenue_micros_d7
	  , SUM(POWER(LEAST(CAST(customer_revenue_micros_d7 AS double)/1e6,500),2)) AS squared_capped_customer_revenue_7
	  , SUM(0) AS expected_val
	  , SUM(0) AS bids
	  , SUM(0) AS aovx_nr_micros
	FROM uncapped_rev_per_auction u
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
)

SELECT
    impression_at
  , click_at
  , install_at
  , at  
  , ab_test_group_id
  , ab_test_group_name
  , exchange
  , customer_id
  , dest_app_id
  , campaign_id
  , country_grouped
  , non_personalized
  , is_viewthrough
  , platform
  , ad_group_type
  , model_type
  , is_exploratory
  , ad_format
  , campaign_tracker_type
  , auction_type
  , spend_based_ab_test_group
  , t2.compay AS customer_name 
  , t3.name AS dest_app_name 
  , t4.display_name AS campaign_name
  , t4.salesforce_campaign_id as salesforce_campaign_id   
  , SUM(impressions) AS impressions
  , SUM(spend_micros) AS spend_micros
  , SUM(revenue_micros) AS revenue_micros
  , SUM(installs) AS installs
  , SUM(target_events_d7) AS target_events_d7
  , SUM(customer_revenue_micros_d7) AS customer_revenue_micros_d7 
  , SUM(target_events_first_d7) AS target_events_first_d7
  , SUM(capped_customer_revenue_micros_d7) AS capped_customer_revenue_micros_d7
  , SUM(squared_capped_customer_revenue_7) AS squared_capped_customer_revenue_7
  , SUM(expected_val) AS expected_val
  , SUM(bids) AS bids
  , SUM(aovx_nr_micros) AS aovx_nr_micros
FROM funnel t1 
LEFT JOIN pinpoint.public.customers t2
  on t1.customer_id = t2.id
LEFT JOIN pinpoint.public.apps t3
  on t1.dest_app_id = t3.id
LEFT JOIN pinpoint.public.campaigns t4
  on t1.campaign_id = t4.id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24