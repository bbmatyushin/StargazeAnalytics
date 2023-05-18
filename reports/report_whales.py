import asyncio

from analytics.analytics_whales import AnalyticsWhales


class WhalesReport:
    def __init__(self):
        self.analytics = AnalyticsWhales()

    async def get_top5_whales(self):
        """ТОП-5 китов Stargaze"""
        url: str = 'https://www.stargaze.zone/profile'  # addr/all
        report: list = []
        num: int = 1
        async for row in self.analytics.top5_whales():
            whale_addr, whale_name, tokens_count = row[0], row[1], row[2]
            whale_name = whale_name if whale_name else f"{whale_addr[:8]}...{whale_addr[-4:]}"
            whale_url = f"{url}/{whale_addr}/all"
            output_ru: str = f"{num}. <a href='{whale_url}'>{whale_name}</a> - {tokens_count:,} NFT\n"
            report.append(output_ru)
            num += 1
        return "".join(report)


async def get_whales_report():
    task1 = asyncio.create_task(WhalesReport().get_top5_whales())
    report1 = await task1
    head: str = "#whales_report\n\n" \
                "<b>🐳 ТОП-5 Китов</b>:\n\n"
    return head + report1


if __name__ == "__main__":
    p = asyncio.run(get_whales_report())
    print(p)
