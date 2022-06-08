import time
from user_agent import generate_user_agent
import asyncio
import aiohttp
from openpyxl import load_workbook
import json



async def parser(row):
    page = 1
    print(page)
    position = 0
    id_item = row[0]
    flag = True
    while flag:
        headers = {
            "Accept": "*/*",
            "User-Agent": generate_user_agent()
        }
        async with aiohttp.ClientSession() as session:
            url = "https://search.wb.ru/exactmatch/ru/common/v4/search" \
                  "?appType=1" \
                  "&curr=rub" \
                  "&dest=-1029256,-102269,-1278703,-1255563" \
                  "&emp=0" \
                  "&lang=ru" \
                  "&locale=ru" \
                  f"&page={page}" \
                  "&pricemarginCoeff=1.0" \
                  f"&query={row[1]}" \
                  "&reg=0" \
                  "&regions=68,64,83,4,38,80,33,70,82,86,75,30,69,22,66,31,48,1,40,71" \
                  "&resultset=catalog&sort=popular" \
                  "&spp=0" \
                  "&stores=117673,122258,122259,125238,125239,125240,6159,507,3158,117501,120602,120762,6158,121709,124731,159402,2737,130744,117986,1733,686,132043,1193"
            print(f"запрос к странице {page} флаг {flag}")
            async with session.get(url=url, headers=headers) as r:

                html_cod = await r.text()
                page += 1
                products = json.loads(html_cod)['data']['products']
                if len(products) > 0:
                    for product in products:
                        id = product['id']
                        position += 1
                        print(f"Запрос {row[1]}\nСтраница {page}\n{id_item}\n{id}\nфлаг {flag}\n--------")
                        if id_item == id:
                            print(f"++++++++\nНайдено совпадение. Позиция в поиске {position}\n{id_item}\n{id}\nфлаг {flag}\n++++++++")
                            flag = False
                            break
                else:
                    flag = False
                    print(f"перебор страниц {page} шт. по запросу {row[1]} закончен. Товар {id_item} не найден флаг {flag}")








async def gahter():
    tasks = []
    wb = load_workbook(filename='1.xlsx')
    sheet = wb.worksheets[2]
    for i, row in enumerate(sheet.values):
        if i > 0:
            if row[1] != None:
                task = asyncio.create_task(parser(row))  # создал задачу
                tasks.append(task)  # добавил её в список
        await asyncio.gather(*tasks)



def main():
    asyncio.get_event_loop().run_until_complete(gahter())

if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Время работы {total_time}")
    # get_id()
