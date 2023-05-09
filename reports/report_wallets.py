from database.user_db_select import UserDBSelect


class WalletsReport:
    """Отчеты по дижениям на кошельках"""
    async def wallet_sales_report(self, owner_addr):
        """Отчет по продажам для определенного кошелька.
        addr_monitor, coll_addr, coll_name, token_name, token_num,
        buyer_addr, buyer_name, price_stars, price_usd, DATETIME(date_create) AS dt"""
        data = await UserDBSelect().select_monitoring_info(addr_monitor=owner_addr)
        if data:
            output_msg = []
            owner_url = f"https://www.stargaze.zone/profile/{owner_addr}/all"
            head_msg = f"#sales_report\n\n" \
                       f"🟢 Продажа с кошелька " \
                       f"<a href='{owner_url}'>{owner_addr[:9]}...{owner_addr[-4:]}</a>\n"
            output_msg.append(head_msg)
            # for data in data_list:
            addr_monitor, coll_addr, coll_name, token_name, token_num, \
                buyer_addr, buyer_name, price_stars, price_usd, dt = data[0], data[1], \
                data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9]
            item_url = f"https://www.stargaze.zone/marketplace/{coll_addr}/{token_num}"
            buyer_url = f"https://www.stargaze.zone/profile/{buyer_addr}/all"
            buyer = buyer_name if buyer_name else f'{buyer_addr[:9]}...{buyer_addr[-4:]}'
            msg = f"\n<b>{coll_name}</b>, <a href='{item_url}'>{token_name}</a>\n" \
                  f"<b>Sale</b>: {price_stars} STARS (${price_usd})\n" \
                  f"<b>Buyer</b>: <a href='{buyer_url}'>{buyer}</a>\n"
            output_msg.append(msg)
            return "".join(output_msg)
        else:
            return None

