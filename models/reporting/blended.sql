{{ config (
    alias = target.database + '_blended'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}


WITH refund_order_data AS
    (SELECT date, day, week, month, quarter, year, 
        order_id, customer_order_index, gross_revenue, total_revenue, subtotal_discount, 0 as subtotal_refund 
    FROM {{ source('reporting','shopify_daily_sales_by_order') }}
    WHERE cancelled_at IS NULL
    UNION ALL
    SELECT date, day, week, month, quarter, year, 
        null as order_id, null as customer_order_index, 0 as gross_revenue, 0 as total_revenue, 0 as subtotal_discount, subtotal_refund 
    FROM {{ source('reporting','shopify_daily_refunds') }} 
    WHERE cancelled_at IS NULL),
    
    initial_sho_data AS (
        {% for granularity in date_granularity_list %}
        SELECT 
            '{{granularity}}' as date_granularity,
            {{granularity}} as date,
            COUNT(DISTINCT order_id) as shopify_orders, 
            COUNT(DISTINCT CASE WHEN customer_order_index = 1 THEN order_id END) as shopify_first_orders,
            SUM(COALESCE(gross_revenue,0)-COALESCE(subtotal_discount,0)) as shopify_sales,
            SUM(CASE WHEN customer_order_index = 1 THEN gross_revenue-subtotal_discount END) as shopify_first_sales,
            COALESCE(SUM(subtotal_refund),0) as shopify_refund,
            SUM(CASE WHEN customer_order_index = 1 THEN subtotal_refund END) as shopify_first_refund
        FROM refund_order_data
        GROUP BY date_granularity, {{granularity}}
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
    ),
    
paid_data as
    (SELECT channel, campaign_id::varchar as campaign_id, campaign_name, date::date, date_granularity, COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, 
        COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(add_to_cart),0) as add_to_cart, COALESCE(SUM(paid_purchases),0) as paid_purchases, COALESCE(SUM(paid_revenue),0) as paid_revenue, 
        0 as shopify_first_orders, 0 as shopify_orders, 0 as shopify_first_sales, 0 as shopify_sales, 0 as shopify_first_net_sales, 0 as shopify_net_sales
    FROM
        (SELECT 'Meta' as channel, campaign_id, campaign_name, date, date_granularity, 
            spend, link_clicks as clicks, impressions, add_to_cart, purchases as paid_purchases, revenue as paid_revenue
        FROM {{ source('reporting','facebook_ad_performance') }}
        UNION ALL
        SELECT 'Google Ads' as channel, campaign_id, campaign_name, date, date_granularity,
            spend, clicks, impressions, add_to_cart, purchases as paid_purchases, revenue as paid_revenue
        FROM {{ source('reporting','googleads_campaign_performance') }}
		UNION ALL
		SELECT 'Tiktok Ads' as channel, campaign_id, campaign_name, date, date_granularity, 
			spend, clicks, impressions, atc as add_to_cart, purchases as paid_purchases, revenue as paid_revenue
        FROM {{ source('reporting','tiktok_campaign_performance') }}
		UNION ALL
		SELECT 'Tiktok Ads' as channel, campaign_id, campaign_name, date_trunc('day',stat_time_day::date)::date as date, 'day' as date_granularity, 
			spend, 0 as clicks, 0 as impressions, 0 as add_to_cart, 0 as paid_purchases, 0 as paid_revenue
        FROM {{ source('tiktok_raw','tiktok_gmv_campaign_performance') }}
        UNION ALL
		SELECT 'Tiktok Ads' as channel, campaign_id, campaign_name, date_trunc('week',stat_time_day::date)::date as date, 'week' as date_granularity, 
			spend, 0 as clicks, 0 as impressions, 0 as add_to_cart, 0 as paid_purchases, 0 as paid_revenue
        FROM {{ source('tiktok_raw','tiktok_gmv_campaign_performance') }}
        UNION ALL
		SELECT 'Tiktok Ads' as channel, campaign_id, campaign_name, date_trunc('month',stat_time_day::date)::date as date, 'month' as date_granularity, 
			spend, 0 as clicks, 0 as impressions, 0 as add_to_cart, 0 as paid_purchases, 0 as paid_revenue
        FROM {{ source('tiktok_raw','tiktok_gmv_campaign_performance') }}
        UNION ALL
		SELECT 'Tiktok Ads' as channel, campaign_id, campaign_name, date_trunc('quarter',stat_time_day::date)::date as date, 'quarter' as date_granularity, 
			spend, 0 as clicks, 0 as impressions, 0 as add_to_cart, 0 as paid_purchases, 0 as paid_revenue
        FROM {{ source('tiktok_raw','tiktok_gmv_campaign_performance') }}
        UNION ALL
		SELECT 'Tiktok Ads' as channel, campaign_id, campaign_name, date_trunc('year',stat_time_day::date)::date as date, 'year' as date_granularity, 
			spend, 0 as clicks, 0 as impressions, 0 as add_to_cart, 0 as paid_purchases, 0 as paid_revenue
        FROM {{ source('tiktok_raw','tiktok_gmv_campaign_performance') }}
        UNION ALL
        SELECT 'Bing' as channel, campaign_id, campaign_name, date, date_granularity, 
            spend, clicks, impressions, 0 as add_to_cart, purchases as paid_purchases, revenue as paid_revenue
        FROM {{ source('reporting','bingads_campaign_performance') }}
        )
    GROUP BY channel, campaign_id, campaign_name, date, date_granularity),

ga4_data as 
    (SELECT campaign_id::varchar as campaign_id, date, date_granularity, 
    sum(sessions) as sessions, sum(engaged_sessions) as engaged_sessions, sum(purchase) as ga4_purchases, sum(purchase_value) as ga4_revenue
    FROM {{ source('reporting','ga4_campaign_performance') }}
    GROUP BY 1,2,3),

paid_ga4_data as (
  SELECT 
    case when campaign_name is null then 'Not Paid' else channel end as channel, campaign_name, date::date, date_granularity,
    SUM(COALESCE(spend, 0)) AS spend,
    SUM(COALESCE(clicks, 0)) AS clicks,
    SUM(COALESCE(impressions, 0)) AS impressions,
	SUM(COALESCE(add_to_cart, 0)) AS add_to_cart,
    SUM(COALESCE(paid_purchases, 0)) AS paid_purchases,
    SUM(COALESCE(paid_revenue, 0)) AS paid_revenue,
    SUM(COALESCE(shopify_first_orders, 0)) AS shopify_first_orders,
    SUM(COALESCE(shopify_orders, 0)) AS shopify_orders,
    SUM(COALESCE(shopify_first_sales, 0)) AS shopify_first_sales,
    SUM(COALESCE(shopify_sales, 0)) AS shopify_sales,
    SUM(COALESCE(shopify_first_net_sales, 0)) AS shopify_first_net_sales,
    SUM(COALESCE(shopify_net_sales, 0)) AS shopify_net_sales,
    SUM(COALESCE(sessions, 0)) AS sessions,
    SUM(COALESCE(engaged_sessions, 0)) AS engaged_sessions,
    SUM(COALESCE(ga4_purchases, 0)) AS ga4_purchases,
    SUM(COALESCE(ga4_revenue, 0)) AS ga4_revenue
  FROM paid_data FULL OUTER JOIN ga4_data USING(date,date_granularity,campaign_id)
  GROUP BY 1,2,3,4),

sho_data as
    (SELECT
            'Shopify' as channel,
            '(not set)' as campaign_name,
            date,
            date_granularity,
            0 as spend,
            0 as clicks,
            0 as impressions,
			0 as add_to_cart,
            0 as paid_purchases,
            0 as paid_revenue, 
            COALESCE(SUM(shopify_first_orders),0) as shopify_first_orders, 
            COALESCE(SUM(shopify_orders),0) as shopify_orders, 
            COALESCE(SUM(shopify_first_sales),0) as shopify_first_sales, 
            COALESCE(SUM(shopify_sales), 0) as shopify_sales,
            COALESCE(SUM(shopify_first_sales),0)-COALESCE(SUM(shopify_first_refund),0) as shopify_first_net_sales,
            COALESCE(SUM(shopify_sales),0)-COALESCE(SUM(shopify_refund),0) as shopify_net_sales,
            0 as sessions,
            0 as engaged_sessions,
            0 as ga4_purchases,
            0 as ga4_revenue 
        FROM initial_sho_data 
        GROUP BY channel, date, campaign_name, date_granularity
    )
    
SELECT 
    channel,
    campaign_name,
    date,
    date_granularity,
    spend,
    clicks,
    impressions,
	add_to_cart,
    paid_purchases,
    paid_revenue,
    shopify_first_orders,
    shopify_orders,
    shopify_first_sales,
    shopify_sales,
    shopify_first_net_sales,
    shopify_net_sales,
    sessions,
    engaged_sessions,
    ga4_purchases,
    ga4_revenue 
FROM (
    SELECT * FROM paid_ga4_data
    UNION ALL 
    SELECT * FROM sho_data
)
