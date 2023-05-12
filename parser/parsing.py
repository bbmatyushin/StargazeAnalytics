import asyncio
import re
# import requests

from aiohttp import ClientSession

# import time
# from fake_headers import Headers
# from bs4 import BeautifulSoup


class Parser:
    def __init__(self):
        self.api_url = 'https://constellations-api.mainnet.stargaze-apis.com/graphql'
        self.header_api = {
            'Content-Type': 'application/json',
            'User-Agent': 'Contact for me by Telegram - @technostarset'
        }
        # self.headers = Headers().generate()

    async def get_collection_data(self, coll_addr: str):
        """Получаем информацию по коллеции"""
        async with ClientSession() as session:
            query = """query Collection {
                  collection(collectionAddr: "<coll_addr>") {
                    collectionAddr
                    floorPrice
                    name
                    tokensCount
                    createdBy {
                      addr
                    }
                  }
                }
                """
            async with session.post(url=self.api_url,
                                    headers=self.header_api,
                                    json={'query': re.sub('<coll_addr>', coll_addr, query)}) as response:

                if response.status == 200:
                    json_data = await response.json()
                    data = json_data.get('data').get('collection')
                    floor_price = data.get("floorPrice")
                    coll_name = data.get("name")
                    tokens_count = data.get("tokensCount")
                    creator_addr = data.get("createdBy").get("addr")
                    return coll_name, floor_price, tokens_count, creator_addr
                # else:
                #     return response.status_code

    async def get_tokens_data(self, coll_addr: str, token_num: str):
        """Получаем информацию о предметах/токенах"""
        async with ClientSession() as session:
            query = """query Tokens {
                      token(collectionAddr: "<coll_addr>", tokenId: "<token_num>") {
                        name
                        rarityOrder
                        tokenId
                        id
                      }
                    }"""
            async with session.post(url=self.api_url, headers=self.header_api,
                                    json={'query': query.replace("<coll_addr>", coll_addr)
                                            .replace("<token_num>", token_num)}) as response:
                if response.status == 200:
                    json_data = await response.json()
                    data = json_data.get('data').get('token')
                    token_name = data.get('name')
                    rarity = data.get("rarityOrder")
                    return token_name, rarity
                else:
                    return response.status

    async def get_owner_name(self, owner_addr: str):
        async with ClientSession() as session:
            query = """query Names{
                      names(ownerAddr: "<owner_addr>") {
                        names {
                          name
                        }
                      }
                    }"""
            async with session.post(url=self.api_url, headers=self.header_api,
                                    json={'query': query.replace("<owner_addr>", owner_addr)}) \
                as response:
                if response.status == 200:
                    json_data = await response.json()
                    name = json_data.get("data").get("names").get("names")
                    owner_name = f'{name[0].get("name")}.stars' if name else None
                    return owner_name
                else:
                    return response.status

    async def get_sales_data(self):
        """Для анализа продаж коллекций"""
        async with ClientSession() as session:
            query_sales = """query Sales {
                          events(filter: SALES, sortBy: BLOCK_HEIGHT_DESC, first: 35) {
                            edges {
                              node {
                                createdAt
                                data
                                blockHeight
                                eventName
                              }
                            }
                          }
                        }"""
            async with session.post(url=self.api_url, headers=self.header_api,
                                    json={'query': query_sales}) as response:
                if response.status == 200:
                    data = await response.json()
                    events_list = data.get('data').get('events').get('edges')
                    for event in events_list:
                        block_height = event["node"]["blockHeight"]
                        coll_addr = event["node"]["data"]["collection"]
                        token_num = event["node"]["data"]["tokenId"]
                        price_usd = float(event["node"]["data"]["priceUsd"])
                        price_stars = int(event["node"]["data"]["price"]) / 1e6
                        seller_addr = event["node"]["data"]["seller"]
                        buyer_addr = event["node"]["data"]["buyer"]
                        date_create = event["node"]["createdAt"]
                        yield block_height, coll_addr, token_num, \
                            price_stars, price_usd, seller_addr, buyer_addr, date_create
                else:
                    yield None

    async def get_burn_data(self):
        """Сбор информации об соженных предметах"""
        async with ClientSession() as session:
            query_burn = """query Burns($filters: [ContractFilter!], $dataFilters: [DataFilter!], $first: Int) {
                          events(contractFilters: $filters, dataFilters: $dataFilters, first: $first) {
                            edges {
                              node {
                                contractType
                                data
                                createdAt
                                blockHeight
                                contractAddr
                              }
                            }
                          }
                        }"""
            variables_burn = {
                            "filters": [
                            {
                              "contractType": "crates.io:sg-721",
                              "events": [
                                {
                                  "name": "wasm",
                                  "action": "burn"
                                }
                              ]
                            },
                            {
                              "contractType": "crates.io:sg721-base",
                              "events": [
                                {
                                  "name": "wasm",
                                  "action": "burn"
                                }
                              ]
                            }
                            ],
                            "dataFilters": [],
                            "first": 25
                            }
            async with session.post(url=self.api_url, headers=self.header_api,
                                    json={'query': query_burn, 'variables': variables_burn}) \
                as response:
                if response.status == 200:
                    data = await response.json()
                    events_list = data.get('data').get('events').get('edges')
                    for event in events_list:
                        block_height = event["node"]["blockHeight"]
                        coll_addr = event["node"]["contractAddr"]
                        token_num = event["node"]["data"]["tokenId"]
                        sender_addr = event["node"]["data"]["sender"]
                        date_create = event["node"]["createdAt"]
                        yield block_height, coll_addr, token_num, sender_addr, date_create
                else:
                    yield None

    async def get_mints_data(self):
        """Получаем данные об минтах"""
        async with ClientSession() as session:
            query = """query Mints{
              events(filter: MINTS, sortBy: BLOCK_HEIGHT_DESC, first: 40) {
                edges {
                  node {
                    createdAt
                    data
                    blockHeight
                    contractInfo
                  }
                }
              }
            }"""
            async with session.post(url=self.api_url, headers=self.header_api,
                               json={'query': query}) as response:
                if response.status == 200:
                    data = await response.json()
                    events_list = data.get('data').get('events').get('edges')
                    for event in events_list:
                        block_height = event["node"].get("blockHeight")
                        coll_addr = event["node"].get("contractInfo").get("sg721_address")
                        token_num = event["node"]["data"].get("tokenId")
                        recipient_addr = event["node"]["data"].get("recipient")
                        creator_addr = event["node"].get("contractInfo").get("admin")
                        price_stars = event["node"]["data"].get("mintPrice")
                        price_stars = int(price_stars) / 1e6 if price_stars else None
                        price_usd = event["node"]["data"].get("mintPriceUsd")
                        price_usd = float(price_usd) if price_usd else None
                        date_create = event["node"].get("createdAt")
                        yield block_height, coll_addr, token_num, recipient_addr, creator_addr, \
                            price_stars, price_usd, date_create


    async def get_listing_and_update_price_data(self):
        """Парсится инфа о новых размещениях и об обновлении
        цен уже на размещенные предметы"""
        async with ClientSession() as session:
            query_listing = """query Sales($first: Int, $contractFilters: [ContractFilter!]) {
                              events(filter: ASKS, sortBy: BLOCK_HEIGHT_DESC, first: $first,
                              contractFilters: $contractFilters) {
                                edges {
                                  node {
                                    createdAt
                                    data
                                    blockHeight
                                    contractAddr
                                    eventName
                                  }
                                }
                              }
                            }"""
            variables_listing = {'first': 20,
                             "contractFilters": [
                                 {
                                     "contractType": "crates.io:sg-marketplace",
                                     "events": [
                                         {
                                             "name": "wasm-update-ask"
                                         }
                                     ]
                                 },
                                 {
                                     "contractType": "crates.io:sg-marketplace",
                                     "events": [
                                         {
                                             "name": "wasm-set-ask"
                                         }
                                     ]
                                 }
                             ]
                             }
            async with session.post(url=self.api_url, headers=self.header_api,
                                    json={'query': query_listing, 'variables': variables_listing}) as response:
                if response.status == 200:
                    data = await response.json()
                    events_list = data.get('data').get('events').get('edges')
                    for event in events_list:
                        block_height = event["node"].get("blockHeight")
                        coll_addr = event["node"]["data"]["collection"]
                        price_stars = int(event["node"]["data"]["price"]) / 1e6
                        price_usd = event["node"]["data"]["priceUsd"]
                        token_num = event["node"]["data"]["tokenId"]
                        seller_addr = event["node"]["data"]["seller"] if event["node"]["data"].get("seller") else None
                        event_name = event["node"].get("eventName")
                        date_create = event["node"]["createdAt"]
                        # sale_type = event["node"]["eventName"]
                        yield block_height, coll_addr, token_num, seller_addr, price_stars, price_usd, \
                            event_name, date_create
                        # yield seller
                else:
                    yield None

    async def owners_parsing(self, owner_addr):
        """По каждому кошельку парсется информация сколко у него токенов
        и из какой коллекции."""
        async with ClientSession() as session:
            query = """query Tokens {
                          tokens(
                            ownerAddr: "<owner_addr>",
                            offset: <offset>,
                            limit: 100
                          ) {
                            tokens {
                              collectionAddr
                              tokenId
                              name
                              forSale
                              createdAt
                            }
                          }
                        }"""
            offset: int = 0
            while True:
                async with session.post(url=self.api_url, headers=self.header_api,
                                        json={'query': query.replace("<owner_addr>", owner_addr)\
                                        .replace('<offset>', str(offset))}) as response:
                    if response.ok:
                        data = await response.json()
                        tokens_data = data.get("data").get("tokens").get("tokens")
                        if tokens_data:
                            for token in tokens_data:
                                coll_addr = token.get('collectionAddr')
                                token_num = token.get('tokenId')
                                token_name = token.get('name')
                                for_sale = token.get('forSale')
                                date_create = token.get('createdAt')
                                yield coll_addr, token_name, token_num, for_sale, date_create
                            await asyncio.sleep(3)
                        else:
                            break
                    else:
                        break
                offset += 100


async def main():
    owner_addr = "stars10e4jhxvuzwcuvy3gsewnzqlmr6pvnp5dfxsvn0"
    # task = asyncio.create_task(Parser().owners_parsing(owner_addr))
    # res = await task
    # print(res)
    count = 0
    async for data in Parser().owners_parsing(owner_addr):
        print(data)
        count += 1
    print(count)


if __name__ == "__main__":
    asyncio.run(main(), debug=True)


