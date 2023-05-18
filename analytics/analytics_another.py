import asyncio
from database.db_create import MainDB


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
    coll_addr = ''
    # result = asyncio.run(AnalyticsBlitz().get_floor_dif(8))
    # print(result)
    # for r in result:
    #     print(r)
