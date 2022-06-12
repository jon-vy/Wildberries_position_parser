import time
from user_agent import generate_user_agent
import asyncio
import aiohttp
from openpyxl import load_workbook
import json
from asyncio import Semaphore
from config import host, user, password,db_name
import pymysql




async def parser(row, semaphore, connection):
    await semaphore.acquire()
    page = 1
    position = 0
    flag = True
    headers = {
        "Accept": "*/*",
        "User-Agent": generate_user_agent()
    }
    async with aiohttp.ClientSession() as session:
        while flag:
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
            # if page == 61:
            #     await asyncio.sleep(10)
            async with session.get(url=url, headers=headers) as r:
                html_cod = await r.text()
                page += 1
                try:
                    products = json.loads(html_cod)['data']['products']
                    if len(products) > 0:

                        for product in products:
                            id = product['id']
                            position += 1
                            # print(f"Запрос {row[1]}\nСтраница {page}\n{row[0]}\n{id}\n--------")
                            if row[0] == id:
                                item_position = position
                                print(f"++++++++\nНайдено совпадение. Позиция в поиске {position}\n{row[0]}\n{id}\n++++++++")
                                flag = False
                                break
                    else:
                        flag = False
                        print(f"По запросу {row[1]} товар {row[0]} не найден")
                        item_position = '-'
                except:
                    flag = False
                    item_position = '-'
                    print(f"По запросу {row[1]} товар {row[0]} не найден")

    with connection.cursor() as cursor:
        insert = f"INSERT INTO `item_position`.`tabl` (`id`, `key`, `position`) VALUES ('{row[0]}', '{row[1]}', '{item_position}');"
        cursor.execute(insert)
        connection.commit()

    semaphore.release()

async def gahter(connection):

    semaphore = Semaphore(30)

    tasks = []
    wb = load_workbook(filename='1.xlsx')
    sheet = wb.worksheets[0]
    for i, row in enumerate(sheet.values):
        if i > 0:
            if row[1] != None:
                task = asyncio.create_task(parser(row, semaphore, connection))  # создал задачу
                tasks.append(task)  # добавил её в список
        await asyncio.gather(*tasks)



def main(connection):
    # clear_base()

    asyncio.get_event_loop().run_until_complete(gahter(connection))




if __name__ == '__main__':
    start_time = time.time()
    print('Подключаюсь к базе')
    input()
    connection = pymysql.connect(
        host="localhost",
        port=3306,
        user="root",
        password="4hYu48AE198L",  # У меня
        # password="J8YHgYpPx7Gq",  # На сервере
        database="item_position",
        cursorclass=pymysql.cursors.DictCursor
    )
    print("Подключился к базе")
    input()
    print("Очистил базу")
    input()
    with connection.cursor() as cursor:
        clear_base = "TRUNCATE tabl;"  # удалит всё содержимое таблицы
        cursor.execute(clear_base)
        connection.commit()

    main(connection)
    connection.close()
    print('закрыл базу')
    input()
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Время работы {total_time}")
    input()
