{{ config (
    alias = target.database + '_openaiads_ad_group_performance'
)}}

{#- conversions -> add_to_cart: the optimization event is `items_added` on both
    live accounts, confirmed in the Ads Manager UI. No purchases / revenue --
    see the bolt-dbt-openaiads-package README. -#}

SELECT
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
ad_group_name,
ad_group_id,
ad_group_status,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as add_to_cart
FROM {{ ref('openaiads_performance_by_ad_group') }}
