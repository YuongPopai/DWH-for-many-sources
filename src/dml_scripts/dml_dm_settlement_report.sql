INSERT INTO cdm.dm_settlement_report (restaurant_id, restaurant_name, settlement_date,
                                                  orders_count, orders_total_sum,
                                                  orders_bonus_payment_sum, orders_bonus_granted_sum,
                                                  order_processing_fee, restaurant_reward_sum)
            SELECT
                r.restaurant_id,
                r.restaurant_name,
                DATE_TRUNC('month', MAX(t.ts)) AS settlement_date,
                COUNT(DISTINCT o.id) AS orders_count,
                GREATEST(SUM(ps.total_sum), 0) AS orders_total_sum,
                GREATEST(SUM(ps.bonus_payment), 0) AS orders_bonus_payment_sum,
                GREATEST(SUM(ps.bonus_grant), 0) AS orders_bonus_granted_sum,
                GREATEST(SUM(ps.total_sum) * 0.25, 0) AS order_processing_fee,
                GREATEST((SUM(ps.total_sum) - SUM(ps.bonus_payment) - (SUM(ps.total_sum) * 0.25)), 0) AS restaurant_reward_sum
            FROM
                dds.dm_orders o
            JOIN
                dds.dm_restaurants r ON o.restaurant_id = r.id
            JOIN
                dds.fct_product_sales ps ON o.id = ps.order_id
            JOIN
                dds.dm_timestamps t ON o.timestamp_id = t.id
            WHERE
                o.order_status = 'CLOSED'
                AND t.ts >= date_trunc('month', CURRENT_DATE) - INTERVAL '1 month'
                AND t.ts < date_trunc('month', CURRENT_DATE)
            GROUP BY
                r.restaurant_id, r.restaurant_name
            ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
            SET
                orders_count = EXCLUDED.orders_count,
                orders_total_sum = GREATEST(EXCLUDED.orders_total_sum, 0),
                orders_bonus_payment_sum = GREATEST(EXCLUDED.orders_bonus_payment_sum, 0),
                orders_bonus_granted_sum = GREATEST(EXCLUDED.orders_bonus_granted_sum, 0),
                order_processing_fee = GREATEST(EXCLUDED.order_processing_fee, 0),
                restaurant_reward_sum = GREATEST(EXCLUDED.restaurant_reward_sum, 0);