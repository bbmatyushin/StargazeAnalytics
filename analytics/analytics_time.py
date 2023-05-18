import asyncio
from database.db_create import MainDB


class AnalyticsTime(MainDB):
    """Аналитика последние 24 часа, 7 дней, 30 дней"""
    def __init__(self, _hours: int):
        """
        :param _hours: 24 (24 Hours), 168 (7 Days), 720 (30 days)
        """
        super().__init__()
        self.hours = _hours

    async def total_volum(self):
        """Общий объём торго на площадке за последние 24 часа"""
        async with self.connector as conn:
            sql = f"""WITH t1 AS(
                        SELECT SUM(price_stars) sum_stars_t1,
                            SUM(price_usd) sum_usd_t1
                        FROM sales
                        WHERE DATETIME(date_create) >= DATETIME(DATETIME(), '-{self.hours} hours')
                    ),
                    t2 AS(
                        SELECT SUM(price_stars) sum_stars_t2,
                            SUM(price_usd) sum_usd_t2
                        FROM sales 
                        WHERE DATETIME(date_create) BETWEEN DATETIME(DATETIME(), '-{self.hours * 2} hours')
                            AND DATETIME(DATETIME(), '-{self.hours} hours')
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
            sql = f"""WITH t1 AS(
                        SELECT sales.coll_id as coll_id, coll_name, coll_addr,
                        SUM(price_stars) sum_stars_t1, SUM(price_usd) sum_usd_t1,
                        COUNT(coll_name) count_coll_t1,
                        AVG(price_stars) avg_stars_t1
                        FROM sales JOIN collections USING(coll_id)
                        WHERE DATETIME(date_create) >= DATETIME(DATETIME(), '-{self.hours} hours')
                        GROUP BY coll_id, coll_name
                        ORDER BY sum_stars_t1 DESC LIMIT ?
                    ),
                    t2 AS(
                        SELECT sales.coll_id as coll_id, coll_name, coll_addr,
                        SUM(price_stars) sum_stars_t2, SUM(price_usd) sum_usd_t2,
                        COUNT(coll_name) count_coll_t2,
                        AVG(price_stars) avg_stars_t2
                        FROM sales JOIN collections USING(coll_id)
                        WHERE DATETIME(date_create) BETWEEN DATETIME(DATETIME(), '-{self.hours * 2} hours')
                            AND DATETIME(DATETIME(), '-{self.hours} hours')
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
                    WHERE DATETIME(date_add) >= DATETIME(DATETIME(), '-{self.hours} hours')
                    GROUP BY coll_id
                    ),
                    floor_t2 AS(
                    SELECT coll_id, floor_price as floor_price_t2
                    FROM get_floor
                    WHERE DATETIME(date_add) < DATETIME(DATETIME(), '-{self.hours} hours')
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
                        END as 'delta_floor, %',
                    ROUND(avg_stars_t1) avg_stars_t1,
                    ROUND(avg_stars_t2) avg_stars_t2,
                    CASE
                        WHEN avg_stars_t2 IS NULL THEN 'nope'
                        WHEN avg_stars_t2 = 0 THEN 'nope'
                        ELSE ROUND((avg_stars_t1 * 1.0/avg_stars_t2 - 1) * 100, 2)
                    END as 'delta_floor, %'
                    FROM t1 LEFT JOIN t2 USING(coll_id)
                    LEFT JOIN floor_t1 USING(coll_id)
                    LEFT JOIN floor_t2 USING(coll_id)"""
            async with conn.execute(sql, (top_num,)) as cursor:
                result = await cursor.fetchall()
                for row in result:
                    yield row
            # return result

    async def buyers_top3(self):
        """ТОП-3 покупателя за последние 24 часа, 7 дней, 30 дней"""
        async with self.connector as conn:
            sql = f"""WITH t1 AS(
                        SELECT owners.owner_id as buyer_id, 
                        owner_name as buyer_name, owner_addr as buyer_addr,
                        SUM(price_stars) sum_stars_t1, SUM(price_usd) sum_usd_t1
                        FROM sales JOIN owners ON sales.buyer_id = owners.owner_id
                        WHERE DATETIME(date_create) >= DATETIME(DATETIME(), '-{self.hours} hours')
                        GROUP BY owner_id, owner_name, owner_addr
                        ORDER BY sum_stars_t1 DESC LIMIT 3
                    ),
                    t2 AS(
                        SELECT owners.owner_id as buyer_id, 
                        owner_name as buyer_name, owner_addr as buyer_addr,
                        SUM(price_stars) sum_stars_t2, SUM(price_usd) sum_usd_t2
                        FROM sales JOIN owners ON sales.buyer_id = owners.owner_id
                        WHERE DATETIME(date_create) BETWEEN DATETIME(DATETIME(), '-{self.hours * 2} hours')
                            AND DATETIME(DATETIME(), '-{self.hours} hours')
                        GROUP BY owner_id, owner_name, owner_addr
                    )
                    SELECT t1.buyer_name, t1.buyer_addr,
                        sum_stars_t1, sum_stars_t2,
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
                        END as 'delta_usd, %'
                    FROM t1 LEFT JOIN t2 USING(buyer_id)"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
                for row in result:
                    yield row
            # return result

    async def sellers_top3(self):
        """ТОП-3 продавца за последние 24 часа
        :param hours - таймфрейм для отчета"""
        async with self.connector as conn:
            sql = f"""WITH t1 AS(
                        SELECT owners.owner_id as seller_id, 
	                        owner_name as seller_name, owner_addr as seller_addr,
                            SUM(price_stars) sum_stars_t1,
                            SUM(price_usd) sum_usd_t1
                        FROM sales JOIN owners ON sales.seller_id = owners.owner_id
                        WHERE DATETIME(date_create) >= DATETIME(DATETIME(), '-{self.hours} hours')
                        GROUP BY owner_id, owner_name, owner_addr
                        ORDER BY sum_stars_t1 DESC LIMIT 3
                    ),
                    t2 AS(
                        SELECT owners.owner_id as seller_id, 
	                        owner_name as seller_name, owner_addr as seller_addr,
                            SUM(price_stars) sum_stars_t2,
                            SUM(price_usd) sum_usd_t2
                        FROM sales JOIN owners ON sales.seller_id = owners.owner_id
                        WHERE DATETIME(date_create) BETWEEN DATETIME(DATETIME(), '-{self.hours * 2} hours')
                            AND DATETIME(DATETIME(), '-{self.hours} hours')
                        GROUP BY owner_id, owner_name, owner_addr
                    )
                    SELECT t1.seller_name, t1.seller_addr,
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
                    FROM t1 LEFT JOIN t2 USING(seller_id)"""
            async with conn.execute(sql) as cursor:
                result = await cursor.fetchall()
                for row in result:
                    yield row
            # return result

if __name__ == "__main__":
    coll_addr = ''
    result = asyncio.run(AnalyticsTime(24).total_volum())
    print(result)
    # for r in result:
    #     print(r)
    #
    # for r in result:
    #     print(r)
