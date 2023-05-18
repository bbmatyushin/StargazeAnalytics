import asyncio
from database.db_create import MainDB


class AnalyticsBlitz(MainDB):
    """Оперативная аналитика по маркетплейсу"""
    async def get_top_blitz(self, top_num):
        """ТОП самых активно торгующихся за последние 10 часов"""
        async with self.connector as conn:
            #TODO: Переделать этот огромный запрос
            sql = """WITH t_now AS(
                        SELECT sales.coll_id as coll_id, coll_name, coll_addr,
                        SUM(price_stars) sum_stars_t1, SUM(price_usd) sum_usd_t1,
                        COUNT(coll_name) count_coll_t1
                        FROM sales JOIN collections USING(coll_id)
                        WHERE DATETIME(date_create) >= DATETIME(DATETIME(), '-1 hours')
                        GROUP BY coll_id, coll_name
                        ORDER BY sum_stars_t1 DESC LIMIT ?
                    ),
                    t_1h AS(
                        SELECT sales.coll_id as coll_id, coll_name, coll_addr,
                        SUM(price_stars) sum_stars_t2, SUM(price_usd) sum_usd_t2,
                        COUNT(coll_name) count_coll_t2
                        FROM sales JOIN collections USING(coll_id)
                        WHERE DATETIME(date_create) BETWEEN DATETIME(DATETIME(), '-2 hours')
                            AND DATETIME(DATETIME(), '-1 hours')
                        GROUP BY coll_id, coll_name
                    ),
                    t_2h AS(
                        SELECT sales.coll_id as coll_id, coll_name, coll_addr,
                        SUM(price_stars) sum_stars_t3, SUM(price_usd) sum_usd_t3,
                        COUNT(coll_name) count_coll_t3
                        FROM sales JOIN collections USING(coll_id)
                        WHERE DATETIME(date_create) BETWEEN DATETIME(DATETIME(), '-3 hours')
                            AND DATETIME(DATETIME(), '-2 hours')
                        GROUP BY coll_id, coll_name
                    ),
                    t_4h AS(
                        SELECT sales.coll_id as coll_id, coll_name, coll_addr,
                        SUM(price_stars) sum_stars_t4, SUM(price_usd) sum_usd_t4,
                        COUNT(coll_name) count_coll_t4
                        FROM sales JOIN collections USING(coll_id)
                        WHERE DATETIME(date_create) BETWEEN DATETIME(DATETIME(), '-5 hours')
                            AND DATETIME(DATETIME(), '-4 hours')
                        GROUP BY coll_id, coll_name
                    ),
                    t_6h AS(
                        SELECT sales.coll_id as coll_id, coll_name, coll_addr,
                        SUM(price_stars) sum_stars_t5, SUM(price_usd) sum_usd_t5,
                        COUNT(coll_name) count_coll_t5
                        FROM sales JOIN collections USING(coll_id)
                        WHERE DATETIME(date_create) BETWEEN DATETIME(DATETIME(), '-7 hours')
                            AND DATETIME(DATETIME(), '-6 hours')
                        GROUP BY coll_id, coll_name
                    ),
                    t_10h AS(
                        SELECT sales.coll_id as coll_id, coll_name, coll_addr,
                        SUM(price_stars) sum_stars_t6, SUM(price_usd) sum_usd_t6,
                        COUNT(coll_name) count_coll_t6
                        FROM sales JOIN collections USING(coll_id)
                        WHERE DATETIME(date_create) BETWEEN DATETIME(DATETIME(), '-11 hours')
                            AND DATETIME(DATETIME(), '-10 hours')
                        GROUP BY coll_id, coll_name
                    )
                    SELECT t_now.coll_name, t_now. coll_addr, sum_stars_t1,
                        CASE
                            WHEN sum_stars_t2 IS NULL THEN 'null'
                            WHEN sum_stars_t2 = 0 THEN 'null'
                            ELSE ROUND((sum_stars_t1 * 1.0/sum_stars_t2 - 1) * 100, 2)
                        END as 'delta_stars_1h, %',
                        CASE
                            WHEN sum_stars_t3 IS NULL THEN 'null'
                            WHEN sum_stars_t3 = 0 THEN 'null'
                            ELSE ROUND((sum_stars_t1 * 1.0/sum_stars_t3 - 1) * 100, 2)
                        END as 'delta_stars_2h, %',
                        CASE
                            WHEN sum_stars_t4 IS NULL THEN 'null'
                            WHEN sum_stars_t4 = 0 THEN 'null'
                            ELSE ROUND((sum_stars_t1 * 1.0/sum_stars_t4 - 1) * 100, 2)
                        END as 'delta_stars_4h, %',
                        CASE
                            WHEN sum_stars_t5 IS NULL THEN 'null'
                            WHEN sum_stars_t5 = 0 THEN 'null'
                            ELSE ROUND((sum_stars_t1 * 1.0/sum_stars_t5 - 1) * 100, 2)
                        END as 'delta_stars_6h, %',
                        CASE
                            WHEN sum_stars_t6 IS NULL THEN 'null'
                            WHEN sum_stars_t6 = 0 THEN 'null'
                            ELSE ROUND((sum_stars_t1 * 1.0/sum_stars_t6 - 1) * 100, 2)
                        END as 'delta_stars_10h, %',
                        count_coll_t1,
                        CASE
                            WHEN count_coll_t2 IS NULL THEN 'null'
                            WHEN count_coll_t2 = 0 THEN 'null'
                            ELSE ROUND((count_coll_t1 * 1.0/count_coll_t2 - 1) * 100, 2)
                        END as 'delta_count_1h, %',
                        CASE
                            WHEN count_coll_t3 IS NULL THEN 'null'
                            WHEN count_coll_t3 = 0 THEN 'null'
                            ELSE ROUND((count_coll_t1 * 1.0/count_coll_t3 - 1) * 100, 2)
                        END as 'delta_count_2h, %',
                        CASE
                            WHEN count_coll_t4 IS NULL THEN 'null'
                            WHEN count_coll_t4 = 0 THEN 'null'
                            ELSE ROUND((count_coll_t1 * 1.0/count_coll_t4 - 1) * 100, 2)
                        END as 'delta_count_4h, %',
                        CASE
                            WHEN count_coll_t5 IS NULL THEN 'null'
                            WHEN count_coll_t5 = 0 THEN 'null'
                            ELSE ROUND((count_coll_t1 * 1.0/count_coll_t5 - 1) * 100, 2)
                        END as 'delta_count_6h, %',
                        CASE
                            WHEN count_coll_t6 IS NULL THEN 'null'
                            WHEN count_coll_t6 = 0 THEN 'null'
                            ELSE ROUND((count_coll_t1 * 1.0/count_coll_t6 - 1) * 100, 2)
                        END as 'delta_count_10h, %'
                    FROM t_now
                        LEFT JOIN t_1h USING(coll_id)
                        LEFT JOIN t_2h USING(coll_id)
                        LEFT JOIN t_4h USING(coll_id)
                        LEFT JOIN t_6h USING(coll_id)
                        LEFT JOIN t_10h USING(coll_id)"""
            async with conn.execute(sql, (top_num,)) as cursor:
                result = await cursor.fetchall()
                for res in result:
                    yield res
            # return result

    async def get_top_blitz_v2(self, top_num):
        """Второй вариант поиска самык активных коллекций
        (основной упор на кол-во проданных предметов)"""
        async with self.connector as conn:
            sql = """WITH action_t AS(
                        SELECT coll_id, STRFTIME('%Y-%m-%d %H:00', date_create) as dt,
                            SUM(price_stars) as sum_stars, 
                            COUNT(coll_id) as count_c,
                            RANK() OVER(PARTITION BY coll_id ORDER BY STRFTIME('%m-%d %H', date_create) DESC) as 'rank'
                        FROM sales
                        GROUP BY coll_id, dt
                        HAVING COUNT(coll_id) > 1
                        ORDER BY dt DESC, count_c DESC
                        ),
                    floor_t AS(
                        SELECT coll_id, STRFTIME('%Y-%m-%d %H:00', date_add) as dt,
                            MIN(floor_price) as floor_price
                        FROM floors
                        GROUP BY coll_id, date_add
                        ),
                    main_t AS(SELECT * FROM action_t
                        LEFT JOIN floor_t USING(coll_id, dt)
                        ORDER BY dt DESC, rank, count_c DESC
                        ),
                    t1 AS(
                        SELECT * FROM main_t
                        WHERE rank = 1
                        LIMIT ?
                        ),
                    t2 AS(
                        SELECT * FROM main_t
                        WHERE rank = 2
                        ),
                    t3 AS(
                        SELECT * FROM main_t
                        WHERE rank = 3
                        ),
                    t4 AS(
                        SELECT * FROM main_t
                        WHERE rank = 4
                        ),
                    t5 AS(
                        SELECT * FROM main_t
                        WHERE rank = 5
                        )
                    SELECT coll_addr, coll_name, t1.dt,
                        t1.count_c,
                        CASE
                            WHEN t2.count_c IS NULL THEN 'nope'
                            WHEN t2.count_c = 0 THEN 0.0
                            ELSE ROUND((t1.count_c * 1.0 / t2.count_c - 1) * 100, 2)
                        END AS 'delta_count2',
                        CASE
                            WHEN t3.count_c IS NULL THEN 'nope'
                            WHEN t3.count_c = 0 THEN 0.0
                            ELSE ROUND((t1.count_c * 1.0 / t3.count_c - 1) * 100, 2)
                        END AS 'delta_count3',
                        CASE
                            WHEN t4.count_c IS NULL THEN 'nope'
                            WHEN t4.count_c = 0 THEN 0.0
                            ELSE ROUND((t1.count_c * 1.0 / t4.count_c - 1) * 100, 2)
                        END AS 'delta_count4',
                        CASE
                            WHEN t5.count_c IS NULL THEN 'nope'
                            WHEN t5.count_c = 0 THEN 0.0
                            ELSE ROUND((t1.count_c * 1.0 / t5.count_c - 1) * 100, 2)
                        END AS 'delta_count5',
                        t1.sum_stars,
                        CASE
                            WHEN t2.sum_stars IS NULL THEN 'nope'
                            WHEN t2.sum_stars = 0 THEN 0.0
                            ELSE ROUND((t1.sum_stars * 1.0 / t2.sum_stars - 1) * 100, 2)
                        END AS 'delta_sum2',
                        CASE
                            WHEN t3.sum_stars IS NULL THEN 'nope'
                            WHEN t3.sum_stars = 0 THEN 0.0
                            ELSE ROUND((t1.sum_stars * 1.0 / t3.sum_stars - 1) * 100, 2)
                        END AS 'delta_sum3',
                        CASE
                            WHEN t4.sum_stars IS NULL THEN 'nope'
                            WHEN t4.sum_stars = 0 THEN 0.0
                            ELSE ROUND((t1.sum_stars * 1.0 / t4.sum_stars - 1) * 100, 2)
                        END AS 'delta_sum4',
                        CASE
                            WHEN t5.sum_stars IS NULL THEN 'nope'
                            WHEN t5.sum_stars = 0 THEN 0.0
                            ELSE ROUND((t1.sum_stars * 1.0 / t5.sum_stars - 1) * 100, 2)
                        END AS 'delta_sum5',
                        t1.floor_price,
                        CASE
                            WHEN t2.floor_price IS NULL THEN 'nope'
                            WHEN t2.floor_price = 0 THEN 0.0
                            ELSE ROUND((t1.floor_price * 1.0 / t2.floor_price - 1) * 100, 2)
                        END AS 'delta_floor2',
                        CASE
                            WHEN t3.floor_price IS NULL THEN 'nope'
                            WHEN t3.floor_price = 0 THEN 0.0
                            ELSE ROUND((t1.floor_price * 1.0 / t3.floor_price - 1) * 100, 2)
                        END AS 'delta_floor3',
                        CASE
                            WHEN t4.floor_price IS NULL THEN 'nope'
                            WHEN t4.floor_price = 0 THEN 0.0
                            ELSE ROUND((t1.floor_price * 1.0 / t4.floor_price - 1) * 100, 2)
                        END AS 'delta_floor4',
                        CASE
                            WHEN t5.floor_price IS NULL THEN 'nope'
                            WHEN t5.floor_price = 0 THEN 0.0
                            ELSE ROUND((t1.floor_price * 1.0 / t5.floor_price - 1) * 100, 2)
                        END AS 'delta_floor5'
                    FROM t1
                        LEFT JOIN t2 USING(coll_id)
                        LEFT JOIN t3 USING(coll_id)
                        LEFT JOIN t4 USING(coll_id)
                        LEFT JOIN t5 USING(coll_id)
                        LEFT JOIN collections USING(coll_id)"""
            async with conn.execute(sql, (top_num,)) as cursor:
                result = await cursor.fetchall()
                for res in result:
                    yield res
            # return result

    async def get_floor_dif(self, tf: int):
        """Получаем коллекции у которых флор менядся последние 9
        промежутков времени по 'tf' часов. Сравниваем на сколько изменилась цена.
        Оставляем только тех, у кого за последние 2 TF цена изменилась больше,
        чем на 2%.
        Плюс выводится количество сминтинных прдеметов

        :param tf - timeframe - промежуток вермени для анализа"""

        async with self.connector as conn:
            sql = f"""WITH t1 AS(SELECT coll_id, 
                        ROUND(AVG(floor_price), 2) AS avg_floor_1
                        FROM floors
                        WHERE date_add BETWEEN DATETIME(DATETIME(), '-{tf} hour') AND DATETIME()
                        GROUP BY coll_id
                        ORDER BY date_add DESC
                        ),
                    t2 AS(SELECT coll_id, 
                        ROUND(AVG(floor_price), 2) AS avg_floor_2,
                        '1' AS flag
                        FROM floors
                        WHERE date_add BETWEEN DATETIME(DATETIME(), '-{tf * 2} hour')
                            AND DATETIME(DATETIME(), '-{tf} hour')
                        GROUP BY coll_id
                        ORDER BY date_add DESC
                        ),
                    t3 AS(SELECT coll_id, 
                        ROUND(AVG(floor_price), 2) AS avg_floor_3
                        FROM floors
                        WHERE date_add BETWEEN DATETIME(DATETIME(), '-{tf * 3} hour')
                            AND DATETIME(DATETIME(), '-{tf * 2} hour')
                        GROUP BY coll_id
                        ORDER BY date_add DESC
                        ),
                    t4 AS(SELECT coll_id, 
                        ROUND(AVG(floor_price), 2) AS avg_floor_4
                        FROM floors
                        WHERE date_add BETWEEN DATETIME(DATETIME(), '-{tf * 4} hour')
                            AND DATETIME(DATETIME(), '-{tf * 3} hour')
                        GROUP BY coll_id
                        ORDER BY date_add DESC
                        ),
                    t5 AS(SELECT coll_id, 
                        ROUND(AVG(floor_price), 2) AS avg_floor_5
                        FROM floors
                        WHERE date_add BETWEEN DATETIME(DATETIME(), '-{tf * 5} hour')
                            AND DATETIME(DATETIME(), '-{tf * 4} hour')
                        GROUP BY coll_id
                        ORDER BY date_add DESC
                        ),
                    t6 AS(SELECT coll_id, 
                        ROUND(AVG(floor_price), 2) AS avg_floor_6
                        FROM floors
                        WHERE date_add BETWEEN DATETIME(DATETIME(), '-{tf * 6} hour')
                            AND DATETIME(DATETIME(), '-{tf * 5} hour')
                        GROUP BY coll_id
                        ORDER BY date_add DESC
                        ),
                    t7 AS(SELECT coll_id, 
                        ROUND(AVG(floor_price), 2) AS avg_floor_7
                        FROM floors
                        WHERE date_add BETWEEN DATETIME(DATETIME(), '-{tf * 7} hour')
                            AND DATETIME(DATETIME(), '-{tf * 6} hour')
                        GROUP BY coll_id
                        ORDER BY date_add DESC
                        ),
                    t8 AS(SELECT coll_id, 
                        ROUND(AVG(floor_price), 2) AS avg_floor_8
                        FROM floors
                        WHERE date_add BETWEEN DATETIME(DATETIME(), '-{tf * 8} hour')
                            AND DATETIME(DATETIME(), '-{tf * 7} hour')
                        GROUP BY coll_id
                        ORDER BY date_add DESC
                        ),
                    t9 AS(SELECT coll_id, 
                        ROUND(AVG(floor_price), 2) AS avg_floor_9
                        FROM floors
                        WHERE date_add BETWEEN DATETIME(DATETIME(), '-{tf * 9} hour')
                            AND DATETIME(DATETIME(), '-{tf * 8} hour')
                        GROUP BY coll_id
                        ORDER BY date_add DESC
                        ),
                    minted AS(
                        SELECT coll_id,
                            COUNT(token_id) AS minted_count
                        FROM mints
                        GROUP BY coll_id
                    ),
                    main_tbl AS(SELECT coll_addr, coll_name, avg_floor_1,
                        ROUND((avg_floor_1/avg_floor_2 - 1) * 100, 2) 'floor_2',
                        ROUND((avg_floor_1/avg_floor_3 - 1) * 100, 2) 'floor_3',
                        CASE
                            WHEN avg_floor_4 IS NULL THEN NULL
                            ELSE(ROUND((avg_floor_1/avg_floor_4 - 1) * 100, 2))
                        END 'floor_4',
                        CASE
                            WHEN avg_floor_5 IS NULL THEN NULL
                            ELSE(ROUND((avg_floor_1/avg_floor_5 - 1) * 100, 2))
                        END 'floor_5',
                        CASE
                            WHEN avg_floor_6 IS NULL THEN NULL
                            ELSE(ROUND((avg_floor_1/avg_floor_6 - 1) * 100, 2))
                        END 'floor_6',
                        CASE
                            WHEN avg_floor_7 IS NULL THEN NULL
                            ELSE(ROUND((avg_floor_1/avg_floor_7 - 1) * 100, 2))
                        END 'floor_7',
                        CASE
                            WHEN avg_floor_8 IS NULL THEN NULL
                            ELSE(ROUND((avg_floor_1/avg_floor_8 - 1) * 100, 2))
                        END 'floor_8',
                        CASE
                            WHEN avg_floor_9 IS NULL THEN NULL
                            ELSE(ROUND((avg_floor_1/avg_floor_9 - 1) * 100, 2))
                        END 'floor_9',
                        COALESCE(minted_count || '/' ||	tokens_count, 'None') AS minted_count
                    FROM t1
                        LEFT JOIN t2 USING(coll_id)
                        LEFT JOIN t3 USING(coll_id)
                        LEFT JOIN t4 USING(coll_id)
                        LEFT JOIN t5 USING(coll_id)
                        LEFT JOIN t6 USING(coll_id)
                        LEFT JOIN t7 USING(coll_id)
                        LEFT JOIN t8 USING(coll_id)
                        LEFT JOIN t9 USING(coll_id)
                        LEFT JOIN collections USING(coll_id)
                        LEFT JOIN minted USING(coll_id)
                    WHERE avg_floor_2 IS NOT NULL 
                        AND avg_floor_3 IS NOT NULL
                    )
                    SELECT * FROM main_tbl
                    WHERE floor_2 > 2 AND floor_3 > 2
                    LIMIT 10"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
                for res in result:
                    yield res
                # return result


if __name__ == "__main__":
    coll_addr = 'stars1203ulnyhvzgex8weh3u4yk8prpq9kuh4hc6n9l58gkwehfwg2t4sqzd36f'
    result = asyncio.run(AnalyticsBlitz().get_floor_dif(8))
    # print(result)
    for r in result:
        print(r)
    #
    # for r in result:
    #     print(r)
