{{ config (
    alias = target.database + '_openaiads_campaign_performance'
)}}

{#- `conversions` in the API is the campaigns optimization event. Confirmed in the
    Ads Manager UI (Herramientas > Conversiones > Eventos de conversión) that both
    live accounts point their only conversion campaign at `items_added` -- hence
    add_to_cart, matching the pinterest/facebook vocabulary.

    No purchases / revenue columns: OpenAI Ads exposes no revenue field, and no
    campaign currently optimizes `order_created`. If one ever does, its conversions
    will mean purchases and this alias must be revisited per client.
    See the bolt-dbt-openaiads-package README. -#}

SELECT
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
campaign_daily_budget,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as add_to_cart
FROM {{ ref('openaiads_performance_by_campaign') }}
