-- Financial Agreement (Installment Plans) Pipeline
-- ------------------------------------------------
-- This query aggregates installment plan data and calculates
-- financial metrics such as discount, down payment, and user segmentation.
--
-- Layer: Analytics

CREATE OR REPLACE TABLE analytics_layer.financial_agreements AS

SELECT
    r.region_name,
    p2.*,

    -- Discount calculation
    ROUND(SUM(
        CASE 
            WHEN p2.total_amount < p2.debt_amount 
            THEN p2.debt_amount - p2.total_amount 
            ELSE 0 
        END
    ), 2) AS discount_amount,

    -- Adjusted down payment
    ROUND(SUM(
        CASE 
            WHEN p2.installment_count = 1 
            THEN p2.total_amount 
            ELSE p2.down_payment 
        END
    ), 2) AS adjusted_down_payment,

    -- User segmentation (anonymized)
    CASE 
        WHEN LOWER(p2.agent_name) = 'digital_channel' THEN 'DIGITAL'
        WHEN LOWER(p2.agent_name) = 'collection_agency' THEN 'THIRD_PARTY'
        WHEN LOWER(p2.agent_name) LIKE '%partner%' THEN 'PARTNER'
        WHEN p2.agent_name IN ('agent_a', 'agent_b', 'agent_c') THEN 'COLLECTION'
        ELSE 'CUSTOMER_SERVICE'
    END AS agent_group

FROM (

    SELECT 
        p1.region_id,
        p1.agreement_id,
        p1.agreement_type,
        p1.agreement_date,
        p1.connection_id,
        p1.customer_id,
        p1.agent_name,
        p1.agent_id,

        ROUND(AVG(p1.debt_amount), 2) AS debt_amount,
        ROUND(AVG(p1.financed_amount), 2) AS financed_amount,
        ROUND(AVG(p1.total_amount), 2) AS total_amount,

        MAX(p1.installment_number) AS installment_count,

        ROUND(SUM(
            CASE 
                WHEN p1.entry_value > 0 AND p1.installment_number = 1 
                THEN p1.installment_value 
                ELSE 0 
            END
        ), 2) AS down_payment,

        MAX(p1.entry_payment_flag) AS entry_payment_flag

    FROM (

        SELECT 
            p.region_id,
            p.agreement_id,
            p.agreement_type,
            p.agreement_date,
            p.connection_id,
            p.customer_id,
            p.agent_name,
            p.agent_id,

            ROUND(AVG(p.debt_value), 2) AS debt_amount,
            ROUND(AVG(p.financed_value), 2) AS financed_amount,
            ROUND(AVG(p.sale_value), 2) AS total_amount,

            p.installment_number,

            ROUND(SUM(p.entry_value), 2) AS entry_value,
            ROUND(SUM(p.installment_value), 2) AS installment_value,

            CASE 
                WHEN p.installment_number = 1 
                     AND MIN(p.payment_date) IS NOT NULL 
                THEN 'Y' 
                ELSE 'N' 
            END AS entry_payment_flag

        FROM trusted_layer.installment_plans p

        WHERE p.is_canceled = 'N'

        GROUP BY 
            p.region_id,
            p.agreement_id,
            p.agreement_type,
            p.agreement_date,
            p.connection_id,
            p.customer_id,
            p.installment_number,
            p.agent_name,
            p.agent_id

    ) p1

    GROUP BY 
        p1.region_id,
        p1.agreement_type,
        p1.agreement_date,
        p1.agreement_id,
        p1.connection_id,
        p1.customer_id,
        p1.agent_name,
        p1.agent_id

) p2

JOIN reference_layer.regions r
    ON r.id = p2.region_id

WHERE p2.agreement_date BETWEEN '2025-01-01' AND '2026-12-31'

GROUP BY 
    r.region_name,
    p2.region_id,
    p2.agreement_id,
    p2.agreement_date,
    p2.connection_id,
    p2.customer_id,
    p2.total_amount,
    p2.debt_amount,
    p2.financed_amount,
    p2.installment_count,
    p2.down_payment,
    p2.entry_payment_flag,
    p2.agreement_type,
    p2.agent_name,
    p2.agent_id;
