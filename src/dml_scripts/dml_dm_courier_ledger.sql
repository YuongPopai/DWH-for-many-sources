INSERT INTO cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month,
                                                 orders_count, orders_total_sum, rate_avg,
                                                 order_processing_fee, courier_order_sum, courier_tips_sum,
                                                 courier_reward_sum)
            SELECT 
                t1.courier_id,
                t1.courier_name,
                t4.year,
                t4."month",
                COUNT(*) AS orders_count,
                SUM(t2."sum") AS orders_total_sum,
                AVG(t2.rate) AS rate_avg,
                SUM(t2."sum") * 0.25 AS order_processing_fee,
                CASE 
                    WHEN AVG(t2.rate) < 4 THEN 
                        GREATEST(0.05 * SUM(t2."sum"), 100) 
                    WHEN AVG(t2.rate) >= 4 AND AVG(t2.rate) < 4.5 THEN 
                        GREATEST(0.07 * SUM(t2."sum"), 150) 
                    WHEN AVG(t2.rate) >= 4.5 AND AVG(t2.rate) < 4.9 THEN 
                        GREATEST(0.08 * SUM(t2."sum"), 175) 
                    ELSE 
                        GREATEST(0.10 * SUM(t2."sum"), 200) 
                END AS courier_order_sum,
                SUM(t2.tip_sum) AS courier_tips_sum,
                SUM(t2."sum") + SUM(t2.tip_sum) * 0.95 AS courier_reward_sum
            FROM 
                dds.dm_couriers t1
            INNER JOIN 
                dds.dm_delivers t2 ON t2.courier_id = t1.id
            INNER JOIN 
                dds.dm_orders t3 ON t2.order_id = t3.id 
            INNER JOIN 
                dds.dm_timestamps t4 ON t3.timestamp_id = t4.id
            WHERE 
                t4.ts >= date_trunc('month', CURRENT_DATE) - INTERVAL '1 month'
                AND t4.ts < date_trunc('month', CURRENT_DATE)
            GROUP BY 
                t1.courier_id, t1.courier_name, t4.year, t4."month"
            ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
            SET
                orders_count = EXCLUDED.orders_count,
                orders_total_sum = GREATEST(EXCLUDED.orders_total_sum, 0),
                rate_avg = EXCLUDED.rate_avg,
                order_processing_fee = GREATEST(EXCLUDED.order_processing_fee, 0),
                courier_order_sum = GREATEST(EXCLUDED.courier_order_sum, 0),
                courier_tips_sum = GREATEST(EXCLUDED.courier_tips_sum, 0),
                courier_reward_sum = GREATEST(EXCLUDED.courier_reward_sum, 0);