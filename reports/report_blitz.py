import asyncio

from datetime import datetime
from database.db_analytics import AnalyticsBlitz


class BlitzReport(AnalyticsBlitz):
    async def get_delta(self, delta_value):
        if isinstance(delta_value, float):
            delta_value = f"+{delta_value}% 📈" if delta_value > 0 else \
                f"{delta_value}%" if delta_value == 0 else f"{delta_value}% 📉"
        return delta_value

    async def report_top_blitz_v1(self, top_num):
        """ТОП самых активно торгующихся за последние 10 часов"""
        url_sg = 'https://www.stargaze.zone/marketplace/'
        report_1_ru: list = []
        num: int = 1
        report_head: str = f"\n⚛️ <b>TОП-{top_num} коллекции</b>:\n"
        report_1_ru.append(report_head)
        async for data in self.get_top_blitz(top_num):
            coll_name, coll_addr, sum_stars_t1, delta_stars_1h, delta_stars_2h, \
                delta_stars_4h, delta_stars_6h, delta_stars_10h, count_coll, \
                delta_count_1h, delta_count_2h, delta_count_4h, delta_count_6h, delta_count_10h = \
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], \
                    data[9], data[10], data[11], data[12], data[13]
            delta_stars_1h = await self.get_delta(delta_stars_1h)
            delta_stars_2h = await self.get_delta(delta_stars_2h)
            delta_stars_4h = await self.get_delta(delta_stars_4h)
            delta_stars_6h = await self.get_delta(delta_stars_6h)
            delta_stars_10h = await self.get_delta(delta_stars_10h)
            output_ru = f"\t{num}. <a href='{url_sg}{coll_addr}'><b>{coll_name}</b></a>\n" \
                        f"\t\t🔸 <b>Vol.</b>: {sum_stars_t1:,} STARS \n" \
                        f"\t\t\t<b>[1h]</b> {delta_stars_1h}, <b>[2h]</b> {delta_stars_2h},\n" \
                        f"\t\t\t<b>[4h]</b> {delta_stars_4h}, <b>[6h]</b> {delta_stars_6h},\n" \
                        f"\t\t\t<b>[10h]</b> {delta_stars_10h}\n" \
                        f"\t\t🔸 <b>Sales</b>: {count_coll} ps. \n" \
                        f"\t\t\t<b>[1h]</b> {delta_count_1h}, <b>[2h]</b> {delta_count_2h},\n" \
                        f"\t\t\t<b>[4h]</b> {delta_count_4h}, <b>[6h]</b> {delta_count_6h},\n" \
                        f"\t\t\t<b>[10h]</b> {delta_count_10h}\n"
            report_1_ru.append(output_ru)
            num += 1
        return "".join(report_1_ru)

    async def report_top_blitz_v2(self, top_num):
        """ТОП коллекций с упором на кол-во проданных предметов"""
        url_sg = 'https://www.stargaze.zone/marketplace/'
        # for url_meta add tail -> &chart_dates=past7days~#theme=night
        url_meta_head = 'https://metabase.constellations.zone/public/dashboard/8281228d-66c0-42d9-83d6-f7e07d05728a?collection='
        url_meta_tail = '&chart_dates=past7days~#theme=night'
        report_v2_ru: list = []
        num: int = 1
        report_head: str = f"\n⚛️ <b>TОП-{top_num} коллекции</b>:\n"
        report_v2_ru.append(report_head)
        async for data in self.get_top_blitz_v2(top_num):
            coll_addr, coll_name, dt, count_c, delta_count2, delta_count3, delta_count4, \
                delta_count5, sum_stars, delta_sum2, delta_sum3, delta_sum4, delta_sum5, \
                floor_price, delta_floor2, delta_floor3, delta_floor4, delta_floor5 = \
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], \
                    data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15], \
                    data[16], data[17]
            delta_count2 = await self.get_delta(delta_count2)
            delta_count3 = await self.get_delta(delta_count3)
            delta_count4 = await self.get_delta(delta_count4)
            delta_count5 = await self.get_delta(delta_count5)
            delta_sum2 = await self.get_delta(delta_sum2)
            delta_sum3 = await self.get_delta(delta_sum3)
            delta_sum4 = await self.get_delta(delta_sum4)
            delta_sum5 = await self.get_delta(delta_sum5)
            delta_floor2 = await self.get_delta(delta_floor2)
            delta_floor3 = await self.get_delta(delta_floor3)
            delta_floor4 = await self.get_delta(delta_floor4)
            delta_floor5 = await self.get_delta(delta_floor5)
            dt = datetime.strptime(dt, '%Y-%m-%d %H:%M').strftime("%d %B, %Hh")
            url_coll = f"{url_sg}{coll_addr}"
            url_meta = f'{url_meta_head}{coll_addr}{url_meta_tail}'
            output_ru = f"\n🔸 <a href='{url_coll}'><b>{coll_name}</b></a>:\n" \
                        f"\t<em>{dt} UTC</em>\n" \
                        f"\t<b>Count</b>: {count_c} ps.\n" \
                        f"\t\t{delta_count2} <b>r2</b>, {delta_count3} <b>r3</b>\n" \
                        f"\t\t{delta_count4} <b>r4</b>, {delta_count5} <b>r5</b>\n" \
                        f"\t<b>Vol</b>: {sum_stars:,} STARS\n" \
                        f"\t\t{delta_sum2} <b>r2</b>, {delta_sum3} <b>r3</b>\n" \
                        f"\t\t{delta_sum4} <b>r4</b>, {delta_sum5} <b>r5</b>\n" \
                        f"\t<b>Floor</b>: {floor_price:,} STARS\n" \
                        f"\t\t{delta_floor2} <b>r2</b>, {delta_floor3} <b>r3</b>\n" \
                        f"\t\t{delta_floor4} <b>r4</b>, {delta_floor5} <b>r5</b>\n" \
                        f"<a href='{url_meta}'><em>{'view on metabase':*^5}</em></a>\n"
            report_v2_ru.append(output_ru)
            num += 1
        return "".join(report_v2_ru)

    async def report_floor_dif(self):
        """Отчет по изменению цены флора"""
        url_sg: str = 'https://www.stargaze.zone/marketplace/'
        url_meta_head: str = 'https://metabase.constellations.zone/public/dashboard/8281228d-66c0-42d9-83d6-f7e07d05728a?collection='
        url_meta_tail: str = '&chart_dates=past7days~#theme=night'
        tf = 8
        report: list = []
        head: str = "#floor_report\n\n" \
                    f"📊 <b>Аналитика цены floor за {tf}Ч</b>\n\n"
        report.append(head)
        async for data in self.get_floor_dif(tf=tf):
            coll_addr, coll_name, floor, floor_2, floor_3, floor_4, floor_5, floor_6, floor_7, \
                floor_8, floor_9, minted_count = \
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], \
                data[9], data[10], data[11]
            floor_dif_2 = await self.get_delta(floor_2)
            floor_dif_3 = await self.get_delta(floor_3)
            floor_dif_4 = await self.get_delta(floor_4)
            floor_dif_5 = await self.get_delta(floor_5)
            floor_dif_6 = await self.get_delta(floor_6)
            floor_dif_7 = await self.get_delta(floor_7)
            floor_dif_8 = await self.get_delta(floor_8)
            floor_dif_9 = await self.get_delta(floor_9)
            url_coll: str = f"{url_sg}{coll_addr}"
            url_meta = f'{url_meta_head}{coll_addr}{url_meta_tail}'
            output_ru: str = f"\n🔸 <a href='{url_coll}'><b>{coll_name}</b></a>\n" \
                             f"<b>AVG Floor</b>: {floor} STARS\n" \
                             f"<b>{tf}H</b> {floor_dif_2}, <b>{tf * 2}H</b> {floor_dif_3},\n" \
                             f"<b>{tf * 3}H</b> {floor_dif_4}, <b>{tf * 4}H</b> {floor_dif_5},\n" \
                             f"<b>{tf * 5}H</b> {floor_dif_6}, <b>{tf * 6}H</b> {floor_dif_7},\n" \
                             f"<b>{tf * 7}H</b> {floor_dif_8}, <b>{tf * 8}H</b> {floor_dif_9},\n" \
                             f"<b>Minted</b>: {minted_count}\n" \
                             f"<a href='{url_meta}'><em>{'view on metabase':*^5}</em></a>\n"
            report.append(output_ru)
        # return "".join(report[:4096])
        return report


async def get_blitz_report():
    # task_1 = asyncio.create_task(BlitzReport().report_top_blitz_v1(7))
    # task_2 = asyncio.create_task(BlitzReport().report_top_blitz_v2(7))
    task_3 = asyncio.create_task(BlitzReport().report_floor_dif())
    # report_1 = await task_1
    # report_2 = await task_2
    report_3 = await task_3
    # header = "#1H_report\n\n" \
    #          "📊 <b>Аналитика Stargaze за последний 1Ч</b>\n\n"
    return report_3


if __name__ == "__main__":
    report = asyncio.run(get_blitz_report())
    print("".join(report[:6]))
