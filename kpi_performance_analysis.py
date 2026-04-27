# Agent Performance KPI Analysis
# ---------------------------------
# This script aggregates customer data quality metrics
# at the agent level and assigns performance tiers
# based on predefined business thresholds.
#
# Technologies: PySpark / Spark SQL
# Layer: Analytics

agent_kpi = spark.sql("""

WITH base_kpi AS (
    SELECT 
        agent_id,
        COUNT(*) AS total_services,

        SUM(CASE 
            WHEN registration_completeness_score = 1.0 THEN 1 
            ELSE 0 
        END) AS complete_registrations,

        ROUND(
            CAST(SUM(CASE 
                WHEN registration_completeness_score = 1.0 THEN 1 
                ELSE 0 
            END) AS DOUBLE) 
            / NULLIF(COUNT(*), 0), 
        4) AS pct_complete_registration

    FROM trusted_layer.customer_data_quality_kpi
    WHERE agent_id IS NOT NULL
    GROUP BY agent_id
),

email_kpi AS (
    SELECT 
        agent_id, 
        COUNT(*) AS total_email_opt_in
    FROM trusted_layer.customer_data_quality_kpi
    WHERE email_flag = 'Y'
    GROUP BY agent_id
),

total_kpi AS (
    SELECT 
        agent_id, 
        COUNT(*) AS total_records
    FROM trusted_layer.customer_data_quality_kpi
    GROUP BY agent_id
),

final AS (
    SELECT 
        base.agent_id,
        base.total_services,
        base.complete_registrations,
        base.pct_complete_registration,

        COALESCE(email.total_email_opt_in, 0) AS total_email_opt_in,
        total.total_records,

        CASE
            WHEN base.pct_complete_registration >= 0.90 THEN '120%'
            WHEN base.pct_complete_registration >= 0.85 THEN '110%'
            WHEN base.pct_complete_registration >= 0.80 THEN '100%'
            WHEN base.pct_complete_registration >= 0.78 THEN '90%'
            WHEN base.pct_complete_registration >= 0.75 THEN '80%'
            WHEN base.total_services = 0 THEN 'NO DATA'
            ELSE 'BELOW TARGET'
        END AS performance_tier

    FROM base_kpi base
    LEFT JOIN email_kpi email 
        ON base.agent_id = email.agent_id
    LEFT JOIN total_kpi total 
        ON base.agent_id = total.agent_id
)

SELECT *
FROM final

""")

# Save final analytics table
agent_kpi.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("mergeSchema", "true") \
    .saveAsTable("analytics_layer.agent_performance_kpi")
