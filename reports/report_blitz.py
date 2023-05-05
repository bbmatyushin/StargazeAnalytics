import asyncio

from database.db_analytics import AnalyticBlitz


class BlitzReport(AnalyticBlitz):
    async def report_top7_coll(self):
        """ТОП-7 самых активно торгующихся за последние 10 часов"""
        url_sg = 'https://www.stargaze.zone/marketplace/'
        report_1_ru: list = []
        num: int = 1
        top_num: int = 7
        report_head: str = f"\n⚛️ <b>TОП-{top_num} коллекции</b>:\n"
        report_1_ru.append(report_head)
        async for data in self.get_top7_coll(top_num):
            coll_name, coll_addr, sum_stars_t1, delta_stars_1h, delta_stars_2h, \
                delta_stars_4h, delta_stars_6h, delta_stars_10h, count_coll, \
                delta_count_1h, delta_count_2h, delta_count_4h, delta_count_6h, delta_count_10h = \
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], \
                    data[9], data[10], data[11], data[12], data[13]
            if isinstance(delta_stars_1h, float):
                delta_stars_1h = f"+{delta_stars_1h}% 📈" if delta_stars_1h > 0 else \
                    f"{delta_stars_1h}%" if delta_stars_1h == 0 else f"{delta_stars_1h}% 📉"
            if isinstance(delta_stars_2h, float):
                delta_stars_2h = f"+{delta_stars_2h}% 📈" if delta_stars_2h > 0 else \
                    f"{delta_stars_2h}%" if delta_stars_2h == 0 else f"{delta_stars_2h}% 📉"
            if isinstance(delta_stars_4h, float):
                delta_stars_4h = f"+{delta_stars_4h}% 📈" if delta_stars_4h > 0 else \
                    f"{delta_stars_4h}%" if delta_stars_4h == 0 else f"{delta_stars_4h}% 📉"
            if isinstance(delta_stars_6h, float):
                delta_stars_6h = f"+{delta_stars_6h}% 📈" if delta_stars_6h > 0 else \
                    f"{delta_stars_6h}%" if delta_stars_6h == 0 else f"{delta_stars_6h}% 📉"
            if isinstance(delta_stars_10h, float):
                delta_stars_10h = f"+{delta_stars_10h}% 📈" if delta_stars_10h > 0 else \
                    f"{delta_stars_10h}%" if delta_stars_10h == 0 else f"{delta_stars_10h}% 📉"
            if isinstance(delta_count_1h, float):
                delta_count_1h = f"+{delta_count_1h}% 📈" if delta_count_1h > 0 else \
                    f"{delta_count_1h}%" if delta_count_1h == 0 else f"{delta_count_1h}% 📉"
            if isinstance(delta_count_2h, float):
                delta_count_2h = f"+{delta_count_2h}% 📈" if delta_count_2h > 0 else \
                    f"{delta_count_2h}%" if delta_count_2h == 0 else f"{delta_count_2h}% 📉"
            if isinstance(delta_count_4h, float):
                delta_count_4h = f"+{delta_count_4h}% 📈" if delta_count_4h > 0 else \
                    f"{delta_count_4h}%" if delta_count_4h == 0 else f"{delta_count_4h}% 📉"
            if isinstance(delta_count_6h, float):
                delta_count_6h = f"+{delta_count_6h}% 📈" if delta_count_6h > 0 else \
                    f"{delta_count_6h}%" if delta_count_6h == 0 else f"{delta_count_6h}% 📉"
            if isinstance(delta_count_10h, float):
                delta_count_10h = f"+{delta_count_10h}% 📈" if delta_count_10h > 0 else \
                    f"{delta_count_10h}%" if delta_count_10h == 0 else f"{delta_count_10h}% 📉"
                
            output_ru = f"\t{num}. <a href='{url_sg}{coll_addr}'><b>{coll_name}</b></a>\n" \
                        f"\t\t↳ <b>Vol.</b>: {sum_stars_t1:,} STARS \n" \
                        f"\t\t\t<b>[1h]</b> {delta_stars_1h}, <b>[2h]</b> {delta_stars_2h},\n" \
                        f"\t\t\t<b>[4h]</b> {delta_stars_4h}, <b>[6h]</b> {delta_stars_6h},\n" \
                        f"\t\t\t<b>[10h]</b> {delta_stars_10h}\n" \
                        f"\t\t↳ <b>Sales</b>: {count_coll} ps. \n" \
                        f"\t\t\t<b>[1h]</b> {delta_count_1h}, <b>[2h]</b> {delta_count_2h},\n" \
                        f"\t\t\t<b>[4h]</b> {delta_count_4h}, <b>[6h]</b> {delta_count_6h},\n" \
                        f"\t\t\t<b>[10h]</b> {delta_count_10h}\n"
            report_1_ru.append(output_ru)
            num += 1
        return "".join(report_1_ru)


async def get_blitz_report():
    task = asyncio.create_task(BlitzReport().report_top7_coll())
    report_1 = await task
    header = "#daily_report\n\n" \
             "📊 <b>Аналитика Stargaze за последние 10Ч</b>\n\n"
    return header + report_1


if __name__ == "__main__":
    result = asyncio.run(get_blitz_report())
    print(result)