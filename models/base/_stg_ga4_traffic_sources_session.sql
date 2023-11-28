{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}

{%- set schema_name, table_name = 'ga4_raw', 'traffic_sources_session' -%}

{%- set exclude_fields = [
   "_fivetran_id"
]
-%}

{%- set fields = adapter.get_columns_in_relation(source(schema_name, table_name))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    |list
                    -%}  
{%- set primary_keys = ['date','profile','source_medium','campaign_name','campaign_id'] -%} 

WITH raw_table AS 
    (SELECT 
        {%- for field in fields %}
        {{ bolt_dbt_ga4_package.get_ga4_clean_field(table_name, field) }}
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
    FROM {{ source(schema_name, table_name) }}
    {% if is_incremental() -%}

    -- this filter will only be applied on an incremental run
    where date >= (select max(date)-3 from {{ this }})

    {% endif %}
    ),

    event_table AS (
            {{ bolt_dbt_ga4_package.get_ga4_events_insights__child_source('events_session') }}
    ),

    staging AS 
    (SELECT *,
        sessions * average_session_duration as session_duration,
        sessions - engaged_sessions as bounced_sessions
    FROM raw_table
    )

SELECT *,
        MAX(_fivetran_synced) over () as last_updated,
        date||'_'||profile||'_'||source_medium||'_'||campaign_name||'_'||campaign_id as unique_key
FROM staging
LEFT JOIN event_table USING(date,profile,source_medium,campaign_name,campaign_id)
