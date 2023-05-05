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
        top_num - сколько выводить из ТОПА (3б 5 и т.д.)"""
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
                    )
                    SELECT t1.coll_name, t1. coll_addr, sum_stars_t1, sum_stars_t2,
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
                        END as 'delta_usd, %',
                        count_coll_t1, count_coll_t2,
                        CASE
                            WHEN count_coll_t2 IS NULL THEN 'NEW!'
                            WHEN count_coll_t2 = 0 THEN 'NEW!'
                            ELSE ROUND((count_coll_t1 * 1.0/count_coll_t2 - 1) * 100, 2)
                        END as 'delta_count, %'
                    FROM t1 LEFT JOIN t2 USING(coll_id)"""
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


if __name__ == "__main__":
    result = asyncio.run(Analytics24H().volum_top_num(3))
    print(result)
    #
    # for r in result:
    #     print(r)
