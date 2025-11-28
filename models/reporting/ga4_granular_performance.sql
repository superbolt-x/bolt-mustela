{{ config (
    alias = target.database + '_ga4_granular_performance'
)}}

SELECT 
profile,
source_medium,
campaign_name,
campaign_id,
adset,
ad,
landing_page,
date,
date_granularity,
new_users,
sessions,
average_session_duration,
engaged_sessions,
bounce_rate,
purchase_value,
total_revenue,
purchase,
session_duration,
bounced_sessions
FROM {{ ref('ga4_performance_granular') }}
