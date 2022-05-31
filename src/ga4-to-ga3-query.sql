-- GA4 to GA3 Format query.
-- This query is developed for readability and not necessarily efficiency.
-- Update PRIOR_DAYS to specify how many days back to pull.
-- Update ga4Raw FROM clause to match your dataset in `<project id>.<dataset>.<table>` format.
-- Update `conversion` to conversion event



-- Pulls raw data from GA4 table
WITH 

ga4Raw AS (

  SELECT
  * 
  FROM `{project_id}.{dataset_id}.{table_prefix}*`
  WHERE PARSE_DATE('%Y%m%d', _table_suffix) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {prior_days} DAY) AND CURRENT_DATE()

),


-- Formats GA4 data into a flat table by user, date, and event name.
ga4Flat AS (
  SELECT
    PARSE_DATE("%Y%m%d", event_date) event_date,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MICROS(event_timestamp)) AS event_time,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MICROS(user_first_touch_timestamp)) AS user_first_touch_time,

    -- Type of Event
    event_name,

    -- User ID
    user_pseudo_id,

    -- User Data
    MAX(device.category) device_category,
    MAX(device.language)	device_language,
    MAX(device.web_info.browser)  device_browser,
    MAX(geo.country) geo_country,
    MAX(geo.region) geo_region,
    MAX(geo.city) geo_city,

    -- Session Data
    IF(event_name = "session_start", 1, 0) entrance,
    IF(event_name = "first_visit", 1, 0) new_user,
    MAX(IF(params.key = 'ga_session_id', params.value.int_value, null)) ga_session_id,
    MAX(IF(params.key = 'ga_session_number', params.value.int_value, null)) ga_session_number,
    CAST(MAX(IF(params.key = 'session_engaged', params.value.string_value, null)) as int64) session_engaged,
    MAX(IF(params.key = 'page_title', params.value.string_value, null)) page_title,
    MAX(IF(params.key = 'page_location' AND event_name = 'page_view', params.value.string_value, null)) pageview_location,
    MAX(IF(params.key = 'page_location' AND event_name = 'session_start', params.value.string_value, null)) landing_page,
    MAX(IF(params.key = "engagement_time_msec", params.value.int_value/1000, 0)) AS engagment_time_sec,

    -- Referral Data
    MAX(IF(params.key = 'source', params.value.string_value, null)) utm_source,
    MAX(IF(params.key = 'medium', params.value.string_value, null)) utm_medium,
    MAX(IF(params.key = 'campaign', params.value.string_value, null)) utm_campaign,

    -- Ecommerce Data
    MAX(ecommerce.transaction_id) ecommerce_transaction_id,
    MAX(IF(ecommerce.purchase_revenue IS NOT NULL, ecommerce.purchase_revenue, 0)) ecommerce_purchase_revenue,


    -- Type of Event
    -- Update `conversion` to conversion event
    COUNTIF(event_name = '{conversion_event}' AND params.key = "page_location") AS conversions

  FROM ga4Raw, UNNEST(event_params) AS params

  GROUP BY event_date, event_name, event_timestamp, user_pseudo_id, user_first_touch_timestamp
),


-- Aggregates flat data into more session focused data.
ga4Sessions AS (
  SELECT

    -- Dimensions
    event_date,
    MAX(IF(landing_page IS NOT NULL, landing_page, "(not set")) landing_page,
    MAX(geo_country) country,
    MAX(geo_region) region,
    MAX(geo_city) city,
    MAX(utm_source) utm_source,
    MAX(utm_medium) utm_medium,
    MAX(utm_campaign) utm_campaign,

    -- Usage Metrics
    COUNT(DISTINCT user_pseudo_id) users,
    COUNT(pageview_location) page_views,
    COUNT(DISTINCT pageview_location) unique_page_views,
    SUM(entrance) entrance,
    SUM(new_user) new_user,
    COUNT(DISTINCT ga_session_id) sessions,
    SUM(engagment_time_sec) engagment_time_sec,

    -- Goal Metrics
    SUM(conversions) conversions,
    SUM(ecommerce_purchase_revenue) ecommerce_revenue,
    COUNT(DISTINCT ecommerce_transaction_id) ecommerce_transactions

  FROM ga4Flat
  GROUP BY event_date, ga_session_id, user_pseudo_id

)


-- Main Query: Formats into final GA3-ish report

SELECT

  event_date date,
  landing_page,

  -- Geography
  IF(country<>'', country, "(not set)") country,
  IF(region<>'', region, "(not set)") region,
  IF(city<>'', city, "(not set)") city,

  -- Channel Info
  IF(utm_source IS NOT NULL, utm_source, "(direct)") utm_source,
  IF(utm_medium IS NOT NULL, utm_medium, "(none)") utm_medium,
  IF(utm_campaign IS NOT NULL, utm_campaign, "(not set)") utm_campaign,

  -- Aggregated Metrics
  SUM(users) users,
  SUM(new_user) new_users,
  SUM(entrance) entrances,
  SUM(sessions) sessions,
  SUM(page_views) page_views,
  SUM(unique_page_views) unique_page_views,
  ROUND(SAFE_DIVIDE(SUM(engagment_time_sec), SUM(sessions)), 2) engagment_time_sec_per_session,
  SUM(conversions) conversions,
  ROUND(SUM(ecommerce_revenue), 2) ecommerce_revenue,
  SUM(ecommerce_transactions) ecommerce_transactions,


FROM ga4Sessions
GROUP BY event_date, landing_page, country, region, city, utm_source, utm_medium, utm_campaign
ORDER BY sessions DESC

