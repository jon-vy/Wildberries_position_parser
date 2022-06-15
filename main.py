import datetime
import time
from user_agent import generate_user_agent
import asyncio
import aiohttp
from openpyxl import load_workbook
import json
from asyncio import Semaphore
import pymysql
from config import *


async def parser(row, semaphore, connection, tabl_name):
    await semaphore.acquire()
    page = 1
    position = 1
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
                  f"&query={row[0]}" \
                  "&reg=0" \
                  "&regions=68,64,83,4,38,80,33,70,82,86,75,30,69,22,66,31,48,1,40,71" \
                  "&resultset=catalog" \
                  "&sort=popular" \
                  "&spp=0" \
                  "&stores=117673,122258,122259,125238,125239,125240,6159,507,3158,117501,120602,120762,6158,121709,124731,159402,2737,130744,117986,1733,686,132043,1193"
            if page < 50:

                async with session.get(url=url, headers=headers) as r:
                    html_cod = await r.text()

                    try:
                        products = json.loads(html_cod)['data']['products']
                        if len(products) > 0:
                            for product in products:
                                id = product['id']
                                today = datetime.datetime.today()
                                time_pars = today.strftime("%Y-%m-%d-%H.%M.%S")
                                with connection.cursor() as cursor:
                                    # insert = f"INSERT INTO `item_position`.`{tabl_name}` ('label', `id`, `key`, `position`, `time_pars`) VALUES ('{row[1]}', '{id}', '{row[0]}', '{position}', '{time_pars}');"
                                    insert = f"INSERT INTO `item_position`.`{tabl_name}` (`label`, `id`, `key`, `position`, `time_pars`) VALUES ('{row[1]}', '{id}', '{row[0]}', '{position}', '{time_pars}');"
                                    #          INSERT INTO `item_position`.`2022-06-14_22.04.27` (`label`, `id`, `key`, `position`, `time_pars`) VALUES ('лл', '1', 'ээ', '22', 'лол');
                                    cursor.execute(insert)
                                    connection.commit()
                                    print(f"Запрос {row[0]}\nСтраница {page}\nid {id}\nПозиция в поиске {position}\n--------")
                                    position += 1
                        else:
                            flag = False
                    except Exception as ex:
                        print(f"ошибка {ex}")
                        flag = False
                    finally:
                        page += 1
            else:
                flag = False
    semaphore.release()

async def gahter(connection, tabl_name):
    semaphore = Semaphore(30)
    tasks = []
    wb = load_workbook(filename='1.xlsx')
    sheet = wb.worksheets[0]
    for i, row in enumerate(sheet.values):
        if i > 0:
            if row[0] != None:
                task = asyncio.create_task(parser(row, semaphore, connection, tabl_name))  # создал задачу
                tasks.append(task)  # добавил её в список
        await asyncio.gather(*tasks)

def main(connection, tabl_name):
    asyncio.get_event_loop().run_until_complete(gahter(connection, tabl_name))

if __name__ == '__main__':
    start_time = time.time()
    print('Подключаюсь к базе')
    # input()
    connection = pymysql.connect(
        host=host,
        port=3306,
        user=user,
        # password="4hYu48AE198L",
        # password="J8YHgYpPx7Gq",
        password=password,
        database=db_name
        # cursorclass=pymysql.cursors.DictCursor
    )
    print("Подключился к базе")
    today = datetime.datetime.today()
    # tabl_name = str(time.time()).replace('.', '')
    tabl_name = today.strftime("%Y-%m-%d_%H.%M.%S")
    with connection.cursor() as cursor:
        create_table = f"CREATE TABLE `{tabl_name}`(`label` TEXT NULL, `id` INT NULL,`key` TEXT NULL,`position` INT NULL,`time_pars` TEXT NULL) COLLATE='utf8mb4_0900_ai_ci';"
        # create_table = f"CREATE TABLE `{tabl_name}`(`id` INT NULL,`key` TEXT NULL,`position` INT NULL,`time_pars` TEXT NULL) COLLATE='utf8mb4_0900_ai_ci';"
        cursor.execute(create_table)
        connection.commit()
        print(f"Таблица создана\nИмя таблицы {tabl_name}")

    main(connection, tabl_name)
    connection.close()
    print('закрыл базу')
    # input()
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Время работы {total_time}\nДля закрытия нажмите Enter")
    input()
