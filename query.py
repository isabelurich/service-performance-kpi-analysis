# Customer service data transformation

service_data = spark.sql("""
SELECT
    a.record_id,
    c.city_id,
    c.city_name,
    c.region_id,
    r.region_name,
    a.service_unit,
    a.service_channel,
    to_date(a.service_date, 'yyyy-MM-dd') AS service_date,
    a.room,
    a.service_time,
    a.agent_id,
    a.ticket_id,
    a.ticket_type,
    a.handling_time,
    a.wait_time,
    a.service_type,
    a.closure_type
FROM raw_layer.service_records a

LEFT JOIN reference_layer.cities c
    ON a.unit_id = c.id

LEFT JOIN reference_layer.regions r
    ON c.region_id = r.id

WHERE 
    YEAR(a.service_date) >= 2025
    AND a.record_id <> 0
""")

# Save curated table
service_data.write.mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .option('mergeSchema', 'true') \
    .saveAsTable('trusted_layer.service_records_2025')
