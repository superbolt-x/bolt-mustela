{{ config (
    alias = target.database + '_ga4_campaign_performance'
)}}

SELECT 
profile,
source_medium,
campaign_name,
campaign_id,
date,
date_granularity,
new_users,
sessions,
average_session_duration,
engaged_sessions,
bounce_rate,
purchase_value,
conversions,
total_revenue,
purchase,
session_duration,
bounced_sessions
FROM {{ ref('ga4_performance_by_campaign') }}
