{{ config (
    alias = target.database + '_ga4_performance_by_campaign_session'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set reject_list = ['date','profile','source_medium','campaign_name',
    'campaign_id','day','week','month','quarter','year','last_updated','unique_key'] -%}
{%- set fields = adapter.get_columns_in_relation(ref('ga4_traffic_sources_session'))
                    |map(attribute="name")
                    |reject("in",reject_list)
                    |list
                    -%}  

WITH 
    {% for date_granularity in date_granularity_list -%}

    performance_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        profile,
        source_medium,
        campaign_name,
        campaign_id,
        {%- for field in fields %}
        COALESCE(SUM("{{ field }}"),0) as "{{ field }}"
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        
    FROM {{ ref('ga4_traffic_sources_session') }}
    GROUP BY 1,2,3,4,5,6)

    {%- if not loop.last %},

    {% endif %}
    {%- endfor %}

{% for date_granularity in date_granularity_list -%}
SELECT * 
FROM performance_{{date_granularity}}
{% if not loop.last %}UNION ALL
{% endif %}
{%- endfor %}
