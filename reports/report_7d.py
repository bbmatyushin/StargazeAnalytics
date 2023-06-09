import asyncio
from analytics.analytics_time import AnalyticsTime


class WeeklyReport:
    """Данные для ежедневных отчетов"""
    def __init__(self):
        self.analytics = AnalyticsTime(_hours=168)

    async def get_delta_value(self, delta_value):
        delta_value = f"+{delta_value}% 📈" if delta_value > 0 else \
            f"nope" if delta_value == 0 else f"{delta_value}% 📉"
        return delta_value

    async def get_total_volume(self):
        """Общие объёмы продаж за последние 24 часа"""
        total_7d = await self.analytics.total_volum()
        # sum_STARS_t1, sum_STARS_t2, delta_STARS, sum_USD_t1, sum_USD_t2, delta_USD
        if isinstance(total_7d[2], float):
            delta_stars = await self.get_delta_value(total_7d[2])
        else:
            delta_stars = total_7d[2]
        if isinstance(total_7d[5], float):
            delta_usd = await self.get_delta_value(total_7d[5])
        else:
            delta_usd = total_7d[5]
        report_1_ru = f"\n⭐️ <b>Общий объём продаж</b>:\n" \
                      f"\t◗ {total_7d[0]:,} STARS ({delta_stars})\n" \
                      f"\t◗ {total_7d[3]:,} USD ({delta_usd})\n"
        return report_1_ru

    async def get_total_top_num(self):
        """Объёмы продаж с разбивкой по коллекциям"""
        url_sg = 'https://www.stargaze.zone/marketplace/'
        # tail for url_metabase -> &chart_dates=past7days~#theme=night
        url_metabase = 'https://metabase.constellations.zone/public/dashboard/8281228d-66c0-42d9-83d6-f7e07d05728a?collection='
        report_2_ru: list = []
        num: int = 1
        top_num: int = 3
        report_head: str = f"\n\n🖼 <b>TОП-{top_num} Коллекции</b>:\n"
        report_2_ru.append(report_head)
        async for data in self.analytics.volum_top_num(top_num):
            coll_name, coll_addr, sum_stars_t1, sum_stars_t2, \
                delta_stars, sum_usd_t1, sum_usd_t2, delta_usd, count_t1, count_t2, delta_count, \
                floor_price_t1, floor_price_t2, delta_floor, avg_stars_t1, avg_stars_t2, delta_avg_stars \
                = data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], \
                data[10], data[11], data[12], data[13], data[14], data[15], data[16]
            url_mb = f"{url_metabase}{coll_addr}&chart_dates=past7days~#theme=night"
            if isinstance(delta_stars, float):
                delta_stars = await self.get_delta_value(delta_stars)
            if isinstance(delta_count, float):
                delta_count = await self.get_delta_value(delta_count)
            if isinstance(delta_floor, float):
                delta_floor = await self.get_delta_value(delta_floor)
                if isinstance(delta_avg_stars, float):
                    delta_avg_stars = await self.get_delta_value(delta_avg_stars)
            output_ru = f"\t▪️ <a href='{url_sg}{coll_addr}'><b>{coll_name}</b></a>\n" \
                        f"\t\t↳ <b>Vol.</b>: {sum_stars_t1:,} STARS ({delta_stars})\n" \
                        f"\t\t↳ <b>Sales</b>: {count_t1} ps. ({delta_count})\n" \
                        f"\t\t↳ <b>avg Price</b>: {int(avg_stars_t1):,} STARS ({delta_avg_stars})\n" \
                        f"\t\t↳ <b>Floor</b>: {floor_price_t1:,} STARS ({delta_floor})\n" \
                        f"\t\t↳ <a href='{url_mb}'><em>view on metabase</em></a>\n\n"
            report_2_ru.append(output_ru)
            num += 1
        return "".join(report_2_ru)

    async def get_buyers_top3(self):
        """ТОП-3 покупателя за послендние 7Д"""
        url: str = 'https://www.stargaze.zone/profile'  # addr/all
        report_3_ru: list = []
        num: int = 1
        report_head: str = "\n👤 <b>ТОП-3 Покупателя</b>:\n"
        report_3_ru.append(report_head)
        async for data in self.analytics.buyers_top3():
            buyer_name, buyer_addr, sum_stars_t1, sum_stars_t2, delta_stars, \
                sum_usd_t1, sum_usd_t2, delta_usd = data[0], data[1], data[2], \
                data[3], data[4], data[5], data[6], data[7]
            buyer = buyer_name if buyer_name else f'{buyer_addr[:8]}...{buyer_addr[-4:]}'
            buyer_url = f'{url}/{buyer_addr}/all'
            output = f"\t{num}. <a href='{buyer_url}'>{buyer}</a> - " \
                     f"{sum_stars_t1:,} STARS\n"
            report_3_ru.append(output)
            num += 1
        return "".join(report_3_ru)

    async def get_sellers_top3(self):
        """ТОП-3 продавца за послендние 7D """
        url: str = 'https://www.stargaze.zone/profile'  # addr/all
        report_4_ru: list = []
        num: int = 1
        report_head: str = "\n👥 <b>ТОП-3 Продавца</b>:\n"
        report_4_ru.append(report_head)
        async for data in self.analytics.sellers_top3():
            seller_name, seller_addr, sum_stars_t1, sum_stars_t2, delta_stars, \
                sum_usd_t1, sum_usd_t2, delta_usd = data[0], data[1], data[2], \
                data[3], data[4], data[5], data[6], data[7]
            seller = seller_name if seller_name else f'{seller_addr[:8]}...{seller_addr[-4:]}'
            seller_url = f'{url}/{seller_addr}/all'
            output = f"\t{num}. <a href='{seller_url}'>{seller}</a> - " \
                     f"{sum_stars_t1:,} STARS\n"
            report_4_ru.append(output)
            num += 1
        return "".join(report_4_ru)


async def get_weekly_report():
    task_1 = asyncio.create_task(WeeklyReport().get_total_volume())
    task_2 = asyncio.create_task(WeeklyReport().get_total_top_num())
    task_3 = asyncio.create_task(WeeklyReport().get_buyers_top3())
    task_4 = asyncio.create_task(WeeklyReport().get_sellers_top3())
    report_1_ru = await task_1
    report_2_ru = await task_2
    report_3_ru = await task_3
    report_4_ru = await task_4
    header = "#7D_report\n\n"\
             "📊 <b>Аналитика Stargaze за последние 7Д</b>\n\n"
    return header + report_1_ru + report_2_ru + report_3_ru + report_4_ru


if __name__ == "__main__":
    res = asyncio.run(get_weekly_report())
    # for r in res:
    print(res)

