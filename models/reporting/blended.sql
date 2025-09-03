{{ config (
    alias = target.database + '_blended'
)}}

WITH data as
    (SELECT 
        channel,
        date::date, 
        date_granularity, 
        COALESCE(SUM(spend),0) as spend, 
        COALESCE(SUM(gross_sales),0) as gross_sales, 
        COALESCE(SUM(subtotal_sales),0) as subtotal_sales,
        COALESCE(SUM(orders),0) as orders,
        COALESCE(SUM(first_orders),0) as first_orders,
        COALESCE(SUM(impressions),0) as impressions, 
        COALESCE(SUM(clicks),0) as clicks, 
        COALESCE(SUM(add_to_cart),0) as add_to_cart,
        COALESCE(SUM(purchases),0) as purchases, 
        COALESCE(SUM(revenue),0) as revenue,
        COALESCE(SUM(ga4_purchases),0) as ga4_purchases,
        COALESCE(SUM(ga4_revenue),0) as ga4_revenue,
        COALESCE(SUM(sessions),0) as sessions
    FROM
        (SELECT 'Meta' as channel, date, date_granularity, 
            spend, 0 as gross_sales, 0 as subtotal_sales, 0 as orders, 0 as first_orders, impressions, link_clicks as clicks, add_to_cart, purchases, revenue, 0 as ga4_purchases, 0 as ga4_revenue, sum(0) as sessions
        FROM {{ source('reporting','facebook_ad_performance') }}
        --WHERE campaign_name !~* 'traffic' OR campaign_name != '[Superbolt] - Brand Awareness - March 2022'
        UNION ALL
        SELECT 'Google Ads' as channel, date, date_granularity, 
            spend, 0 as gross_sales, 0 as subtotal_sales, 0 as orders, 0 as first_orders, impressions, clicks, add_to_cart, purchases, revenue, 0 as ga4_purchases, 0 as ga4_revenue, sum(0) as sessions
        FROM {{ source('reporting','googleads_campaign_performance') }}
        UNION ALL
        SELECT 'Bing' as channel, date, date_granularity, 
            spend, 0 as gross_sales, 0 as subtotal_sales, 0 as orders, 0 as first_orders, impressions, clicks, 0 as add_to_cart, purchases, revenue, 0 as ga4_purchases, 0 as ga4_revenue, sum(0) as sessions
        FROM {{ source('reporting','bingads_campaign_performance') }}
        UNION ALL
        SELECT 'Shopify' as channel, date, date_granularity, 
            sum(0) as spend, sum(gross_sales) as gross_sales, sum(subtotal_sales) as subtotal_sales, sum(orders) as orders, sum(first_orders) as first_orders, 0 as impressions, 
            0 as clicks, 0 as add_to_cart, 0 as purchases, 0 as revenue, sum(0) as ga4_purchases, sum(0) as ga4_revenue, sum(0) as sessions
        FROM {{ source('reporting','shopify_sales') }}
        GROUP BY 1,2,3
        UNION ALL
        SELECT 'Meta' as channel, date, date_granularity, 
            sum(0) as spend, sum(0) as gross_sales, sum(0) as subtotal_sales, sum(0) as orders, sum(0) as first_orders, sum(0) as impressions, sum(0) as clicks, sum(0) as add_to_cart, sum(0) as purchases, sum(0) as revenue,
            sum(purchase) as ga4_purchases, sum(purchase_value) as ga4_revenue, sum(sessions) as sessions
        FROM {{ source('reporting','ga4_campaign_performance') }}
        WHERE source_medium = 'facebook / paid'
        GROUP BY 1,2,3
        UNION ALL
        SELECT 'Google Ads' as channel, date, date_granularity, 
            sum(0) as spend, sum(0) as gross_sales, sum(0) as subtotal_sales, sum(0) as orders, sum(0) as first_orders, sum(0) as impressions, sum(0) as clicks, sum(0) as add_to_cart, sum(0) as purchases, sum(0) as revenue,
            sum(purchase) as ga4_purchases, sum(purchase_value) as ga4_revenue, sum(sessions) as sessions
        FROM {{ source('reporting','ga4_campaign_performance') }}
        WHERE source_medium = 'google / cpc'
        GROUP BY 1,2,3
        )
    GROUP BY channel, date, date_granularity)
    
SELECT * FROM
{{ source('utilities','dates') }}
LEFT JOIN data USING (date)
WHERE date <= current_date
