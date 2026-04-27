# Customer Data Quality KPI Pipeline
# ---------------------------------
# This script calculates a customer data completeness score
# based on multiple attributes (name, document, contact, address).
#
# It includes region-specific logic where some regions use
# alternative data sources for address information.
#
# Technologies: PySpark / Spark SQL
# Layer: Trusted

customer_kpi = spark.sql("""

WITH base_intermediate AS (
    SELECT 
        s.record_id,
        s.service_unit,
        s.region_id,
        s.service_channel,
        s.room,
        s.region_name,
        s.agent_id,
        s.city_id,
        s.service_date,

        CONCAT(CAST(s.record_id AS STRING), '_', CAST(s.region_id AS STRING)) AS record_region_key,

        c.connection_id,
        c.customer_id,
        c.delivery_type_id,
        c.delivery_type,
        c.customer_name,
        c.customer_document_1,
        c.customer_document_2,
        c.phone_landline,
        c.phone_mobile,
        c.email,
        c.state,

        -- Normalize region for conditional logic
        UPPER(TRIM(s.region_name)) AS region_name_upper,

        ROW_NUMBER() OVER (
            PARTITION BY s.record_id, s.region_id
            ORDER BY s.service_date DESC
        ) AS rn

    FROM trusted_layer.service_records_2025 s

    LEFT JOIN trusted_layer.customer_connections c 
        ON s.record_id = c.connection_id
       AND s.region_id = c.region_id
),

base AS (
    SELECT 
        bi.*,

        -- Region-specific address logic (anonymized)
        CASE 
            WHEN bi.region_name_upper = 'SPECIAL_REGION' THEN conn.address
            ELSE cust.address
        END AS customer_address,

        CASE 
            WHEN bi.region_name_upper = 'SPECIAL_REGION' THEN conn.neighborhood
            ELSE cust.neighborhood
        END AS customer_neighborhood,

        CASE 
            WHEN bi.region_name_upper = 'SPECIAL_REGION' THEN conn.zip_code
            ELSE cust.zip_code
        END AS customer_zip_code,

        CASE 
            WHEN bi.region_name_upper = 'SPECIAL_REGION' THEN conn.city
            ELSE cust.city
        END AS customer_city

    FROM base_intermediate bi

    LEFT JOIN trusted_layer.customers cust
        ON bi.customer_id = cust.customer_id
       AND bi.region_id = cust.region_id

    LEFT JOIN trusted_layer.customer_connections conn
        ON bi.connection_id = conn.connection_id
       AND bi.region_id = conn.region_id

    WHERE rn = 1
),

data_quality_flags AS (
    SELECT *,
        CASE WHEN TRIM(COALESCE(customer_name, '')) <> '' THEN 1 ELSE 0 END AS has_name,

        CASE WHEN TRIM(COALESCE(customer_document_1, '')) <> '' 
           OR TRIM(COALESCE(customer_document_2, '')) <> '' THEN 1 ELSE 0 END AS has_document,

        CASE WHEN TRIM(COALESCE(phone_landline, '')) <> '' 
           OR TRIM(COALESCE(phone_mobile, '')) <> '' THEN 1 ELSE 0 END AS has_phone,

        CASE WHEN TRIM(COALESCE(customer_address, '')) <> '' THEN 1 ELSE 0 END AS has_address,

        CASE WHEN TRIM(COALESCE(customer_neighborhood, '')) <> '' THEN 1 ELSE 0 END AS has_neighborhood,

        CASE WHEN TRIM(COALESCE(customer_zip_code, '')) <> '' THEN 1 ELSE 0 END AS has_zip_code,

        CASE WHEN TRIM(COALESCE(customer_city, '')) <> '' THEN 1 ELSE 0 END AS has_city,

        CASE WHEN TRIM(COALESCE(state, '')) <> '' THEN 1 ELSE 0 END AS has_state

    FROM base
),

final AS (
    SELECT *,
        ROUND(
            (
                has_name +
                has_document +
                has_phone +
                has_address +
                has_neighborhood +
                has_zip_code +
                has_city +
                has_state
            ) / 8.0,
        4) AS registration_completeness_score
    FROM data_quality_flags
)

SELECT *
FROM final

""")

# Save final table
customer_kpi.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("mergeSchema", "true") \
    .saveAsTable("trusted_layer.customer_data_quality_kpi")
