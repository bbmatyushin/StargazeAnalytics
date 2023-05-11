import asyncio
from database.db_create import MainDB


class Analytics24H(MainDB):
    """Аналитика последние 24 часа"""
    async def total_volum(self):
        """Общий объём торго на площадке за последние 24 часа"""
        async with self.connector as conn:
            sql = """WITH t1 AS(
                        SELECT SUM(price_stars) sum_stars_t1,
                            SUM(price_usd) sum_usd_t1
                        FROM sales
                        WHERE DATETIME(date_create) >= DATETIME(DATETIME(), '-24 hours')
                    ),
                    t2 AS(
                        SELECT SUM(price_stars) sum_stars_t2,
                            SUM(price_usd) sum_usd_t2
                        FROM sales 
                        WHERE DATETIME(date_create) BETWEEN DATETIME(DATETIME(), '-48 hours')
                            AND DATETIME(DATETIME(), '-24 hours')
                    )
                    SELECT sum_stars_t1, sum_stars_t2,
                        CASE
                            WHEN sum_stars_t2 IS NULL THEN 'NEW!'
                            WHEN sum_stars_t2 = 0 THEN 'NEW!'
                            ELSE ROUND((sum_stars_t1 * 1.0/sum_stars_t2 - 1) * 100, 2)
                        END as 'delta_stars, %',
                        ROUND(sum_usd_t1, 2) sum_usd_t1,
                        ROUND(sum_usd_t2, 2) sum_usd_t2,
                        CASE
                            WHEN sum_usd_t2 IS NULL THEN 'NEW!'
                            WHEN sum_usd_t2 = 0 THEN 'NEW!'
                            ELSE ROUND((sum_usd_t1 * 1.0/sum_usd_t2 - 1) * 100, 2)
                        END as 'delta_usd, %'
                    FROM t1, t2"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchone()
            return result

    async def volum_top_num(self, top_num: int):
        """Объемы продаж и количество сделок
        за последние 24 часа с разбивкой по ТОП-5 коллекциям
        top_num - сколько выводить из ТОПА (3, 5 и т.д.)"""
        async with self.connector as conn:
            sql = """WITH t1 AS(
                        SELECT sales.coll_id as coll_id, coll_name, coll_addr,
                        SUM(price_stars) sum_stars_t1, SUM(price_usd) sum_usd_t1,
                        COUNT(coll_name) count_coll_t1
                        FROM sales JOIN collections USING(coll_id)
                        WHERE DATETIME(date_create) >= DATETIME(DATETIME(), '-24 hours')
                        GROUP BY coll_id, coll_name
                        ORDER BY sum_stars_t1 DESC LIMIT ?
                    ),
                    t2 AS(
                        SELECT sales.coll_id as coll_id, coll_name, coll_addr,
                        SUM(price_stars) sum_stars_t2, SUM(price_usd) sum_usd_t2,
                        COUNT(coll_name) count_coll_t2
                        FROM sales JOIN collections USING(coll_id)
                        WHERE DATETIME(date_create) BETWEEN DATETIME(DATETIME(), '-48 hours')
                            AND DATETIME(DATETIME(), '-24 hours')
                        GROUP BY coll_id, coll_name
                    ),
                    get_floor AS(
                    SELECT coll_id, floor_price, date_add
                    FROM floors
                    ORDER BY date_add DESC
                    ),
                    floor_t1 AS(
                    SELECT coll_id, floor_price as floor_price_t1
                    FROM get_floor
                    WHERE DATETIME(date_add) >= DATETIME(DATETIME(), '-24 hours')
                    GROUP BY coll_id
                    ),
                    floor_t2 AS(
                    SELECT coll_id, floor_price as floor_price_t2
                    FROM get_floor
                    WHERE DATETIME(date_add) < DATETIME(DATETIME(), '-24 hours')
                    GROUP BY coll_id
                    )
                    SELECT t1.coll_name, t1. coll_addr, sum_stars_t1, sum_stars_t2,
                        CASE
                            WHEN sum_stars_t2 IS NULL THEN 'nope'
                            WHEN sum_stars_t2 = 0 THEN 'nope'
                            ELSE ROUND((sum_stars_t1 * 1.0/sum_stars_t2 - 1) * 100, 2)
                        END as 'delta_stars, %',
                        ROUND(sum_usd_t1, 2) sum_usd_t1,
                        ROUND(sum_usd_t2, 2) sum_usd_t2,
                        CASE
                            WHEN sum_usd_t2 IS NULL THEN 'nope'
                            WHEN sum_usd_t2 = 0 THEN 'nope'
                            ELSE ROUND((sum_usd_t1 * 1.0/sum_usd_t2 - 1) * 100, 2)
                        END as 'delta_usd, %',
                        count_coll_t1, count_coll_t2,
                        CASE
                            WHEN count_coll_t2 IS NULL THEN 'nope'
                            WHEN count_coll_t2 = 0 THEN 'nope'
                            ELSE ROUND((count_coll_t1 * 1.0/count_coll_t2 - 1) * 100, 2)
                        END as 'delta_count, %',
                        floor_price_t1, floor_price_t2,
                        CASE
                            WHEN floor_price_t2 IS NULL THEN 'nope'
                            WHEN floor_price_t2 = 0 THEN 'nope'
                            ELSE ROUND((floor_price_t1 * 1.0/floor_price_t2 - 1) * 100, 2)
                        END as 'delta_floor, %'
                    FROM t1 LEFT JOIN t2 USING(coll_id)
                    LEFT JOIN floor_t1 USING(coll_id)
                    LEFT JOIN floor_t2 USING(coll_id)"""
            async with conn.execute(sql, (top_num,)) as cursor:
                result = await cursor.fetchall()
                for row in result:
                    yield row
            # return result

    async def whale_top3(self):
        """ТОП-3 кита покупателя за последние 24 часа"""
        async with self.connector as conn:
            sql = """WITH t1 AS(
                        SELECT owners.owner_id as buyer_id, 
                        owner_name as buyer_name, owner_addr as buyer_addr,
                        SUM(price_stars) sum_stars_t1, SUM(price_usd) sum_usd_t1,
                        MIN(DATETIME(date_create)), MAX(DATETIME(date_create))
                        FROM sales JOIN owners ON sales.buyer_id = owners.owner_id
                        WHERE DATETIME(date_create) >= DATETIME(DATETIME(), '-24 hours')
                        GROUP BY owner_id, owner_name, owner_addr
                        ORDER BY sum_stars_t1 DESC LIMIT 3
                    ),
                    t2 AS(
                        SELECT owners.owner_id as buyer_id, 
                        owner_name as buyer_name, owner_addr as buyer_addr,
                        SUM(price_stars) sum_stars_t2, SUM(price_usd) sum_usd_t2,
                        MIN(DATETIME(date_create)), MAX(DATETIME(date_create))
                        FROM sales JOIN owners ON sales.buyer_id = owners.owner_id
                        WHERE DATETIME(date_create) BETWEEN DATETIME(DATETIME(), '-48 hours')
                            AND DATETIME(DATETIME(), '-24 hours')
                        GROUP BY owner_id, owner_name, owner_addr
                    )
                    SELECT t1.buyer_name, t1.buyer_addr,
                        sum_stars_t1, sum_stars_t2,
                        CASE
                            WHEN sum_stars_t2 IS NULL THEN 'NEW!'
                            WHEN sum_stars_t2 = 0 THEN 'NEW!'
                            ELSE ROUND((sum_stars_t1 * 1.0/sum_stars_t2 - 1) * 100, 2)
                        END as 'delta_stars, %',
                        ROUND(sum_usd_t1, 2) sum_usd_t1,
                        ROUND(sum_usd_t2, 2) sum_usd_t2,
                        CASE
                            WHEN sum_usd_t2 IS NULL THEN 'NEW!'
                            WHEN sum_usd_t2 = 0 THEN 'NEW!'
                            ELSE ROUND((sum_usd_t1 * 1.0/sum_usd_t2 - 1) * 100, 2)
                        END as 'delta_usd, %'
                    FROM t1 LEFT JOIN t2 USING(buyer_id)"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
                for row in result:
                    yield row
            # return result


class AnalyticsAnother(MainDB):
    """Разная аналитика"""
    async def top_5_coll_sales_1days(self, days: int):
        """ТОП-5 коллекций по продажам в сравнении за выбранный период
        :param days > 0
        """
        async with self.connector as conn:
            sql = f"""WITH t1 AS(
                        SELECT sales.coll_id, coll_name,
                        SUM(price_stars) t1_sum_stars,
                        ROUND(SUM(price_usd), 2) t1_sum_usd,
                        STRFTIME('%Y-%m-%d', date_create) dt
                        FROM sales 
                        LEFT JOIN collections USING(coll_id)
                        WHERE DATE(date_create) BETWEEN DATE(DATE(), '-{days} days')
                            AND DATE(DATE(), '-1 days')
                        GROUP BY coll_name
                        ORDER BY 3 DESC LIMIT 5
                    ),
                    t2 AS(
                        SELECT sales.coll_id, coll_name,
                        SUM(price_stars) t2_sum_stars,
                        ROUND(SUM(price_usd), 2) t2_sum_usd,
                        STRFTIME('%Y-%m-%d', date_create) dt
                        FROM sales 
                        LEFT JOIN collections USING(coll_id)
                        WHERE DATE(date_create) BETWEEN DATE(DATE(), '-{days + days} days') 
                            AND DATE(DATE(), '-{days + 1} days')
                        GROUP BY coll_name
                    )
                    SELECT t1.coll_name, t1_sum_stars, t2_sum_stars,
                    CASE
                        WHEN t2_sum_stars IS NULL THEN 0
                        ELSE ROUND((t1_sum_stars * 1.0/ t2_sum_stars - 1) * 100, 2)
                    END AS delta_stars,
                    t1_sum_usd, t2_sum_usd,
                    CASE
                        WHEN t1_sum_usd IS NULL THEN 0
                        ELSE ROUND((t1_sum_usd * 1.0/ t2_sum_usd - 1) * 100, 2)
                    END AS delta_usd
                    FROM t1 LEFT JOIN t2 USING(coll_id)"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
        return result


class AnalyticBlitz(MainDB):
    """Оперативная аналитика по маркетплейсу"""
    async def get_top_blitz(self, top_num):
        """ТОП-7 самых активно торгующихся за последние 10 часов"""
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
        """Получаем коллекции у которых флор менядся последние 5
        промежутков времени по 12 часов. Сравниваем на сколько изменилась цена.

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
                        )
                    SELECT coll_addr, coll_name, avg_floor_1,
                        ROUND((avg_floor_1/avg_floor_2 - 1) * 100, 2) 'floor_2, %',
                        ROUND((avg_floor_1/avg_floor_3 - 1) * 100, 2) 'floor_3, %',
                        ROUND((avg_floor_1/avg_floor_4 - 1) * 100, 2) 'floor_4, %',
                        ROUND((avg_floor_1/avg_floor_5 - 1) * 100, 2) 'floor_5, %',
                        CASE
                            WHEN avg_floor_6 IS NULL THEN NULL
                            ELSE(ROUND((avg_floor_1/avg_floor_6 - 1) * 100, 2))
                        END 'floor_6, %',
                        CASE
                            WHEN avg_floor_7 IS NULL THEN NULL
                            ELSE(ROUND((avg_floor_1/avg_floor_7 - 1) * 100, 2))
                        END 'floor_7, %',
                        CASE
                            WHEN avg_floor_8 IS NULL THEN NULL
                            ELSE(ROUND((avg_floor_1/avg_floor_8 - 1) * 100, 2))
                        END 'floor_8, %',
                        CASE
                            WHEN avg_floor_9 IS NULL THEN NULL
                            ELSE(ROUND((avg_floor_1/avg_floor_9 - 1) * 100, 2))
                        END 'floor_9, %'
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
                    WHERE avg_floor_2 IS NOT NULL 
                        AND avg_floor_3 IS NOT NULL
                        AND avg_floor_4 IS NOT NULL
                        AND avg_floor_5 IS NOT NULL"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
                for res in result:
                    yield res
                # return result


if __name__ == "__main__":
    result = asyncio.run(AnalyticBlitz().get_floor_dif())
    for r in result:
        print(r)
    #
    # for r in result:
    #     print(r)
