-- query for creating tableau dash, pulls from dagger pipeline

SELECT
    datetrunc('day',from_iso8601_timestamp(impression_at)) AS impression_at
  , datetrunc('day',from_iso8601_timestamp(dt)) AS dt
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
  , customer_name
  , dest_app_name
  , campaign_name
  , t2.revenue_type 
  , t2.current_optimization_state
  , t4.scale_zone__c as scale_zone
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
FROM product_analytics.cogs_aware_bidding t1 
LEFT JOIN pinpoint_hourly.campaigns t2 
    ON t1.campaign_id = t2.id
    AND t1.dt = t2.dt 
LEFT JOIN salesforce_daily.customer_campaign__c t3
    ON t3.campaign_id_18_digit__c =  t1.salesforce_campaign_id
    AND date_trunc('day',from_iso8601_timestamp(t1.dt)) = from_iso8601_timestamp(t3.dt)
LEFT JOIN salesforce_daily.opportunity t4
    ON t4.opportunity_id_18_digit__c = t3.opportunity__c
    AND date_trunc('day',from_iso8601_timestamp(t1.dt)) = from_iso8601_timestamp(t4.dt)
WHERE t1.dt >= '2023-08-15T22:00'
  AND t1.impression_at >= '2023-08-15T22:00'
  AND t2.dt >= '2023-08-15T22:00'
  AND t3.dt >= '2023-08-15'
  AND t4.dt >= '2023-08-15'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24