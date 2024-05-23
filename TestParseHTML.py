from pymssql import Date
import requests as req
from bs4 import BeautifulSoup
import pymssql
import pandas as pd
# import csv
# import re
import cleantext as ct


SERVER = r'DELL-T7810\SQL2019'
DATABASE = 'Courts'
HOST = r'https://www.mos-gorsud.ru/'
URL = r'https://www.mos-gorsud.ru/rs/'
HEADERS = {
    'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0'
    }

courts = [
{'url':'https://www.mos-gorsud.ru/mgs','name':'Московский городской суд'},
{'url':'https://www.mos-gorsud.ru/rs/babushkinskij','name':'Бабушкинский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/basmannyj','name':'Басманный районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/butyrskij','name':'Бутырский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/gagarinskij','name':'Гагаринский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/golovinskij','name':'Головинский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/dorogomilovskij','name':'Дорогомиловский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/zamoskvoreckij','name':'Замоскворецкий районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/zelenogradskij','name':'Зеленоградский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/zyuzinskij','name':'Зюзинский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/izmajlovskij','name':'Измайловский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/koptevskij','name':'Коптевский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/kuzminskij','name':'Кузьминский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/kuncevskij','name':'Кунцевский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/lefortovskij','name':'Лефортовский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/lyublinskij','name':'Люблинский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/meshchanskij','name':'Мещанский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/nagatinskij','name':'Нагатинский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/nikulinskij','name':'Никулинский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/ostankinskij','name':'Останкинский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/perovskij','name':'Перовский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/preobrazhenskij','name':'Преображенский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/presnenskij','name':'Пресненский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/savyolovskij','name':'Савёловский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/simonovskij','name':'Симоновский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/solncevskij','name':'Солнцевский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/taganskij','name':'Таганский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/tverskoj','name':'Тверской районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/timiryazevskij','name':'Тимирязевский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/troickij','name':'Троицкий районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/tushinskij','name':'Тушинский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/hamovnicheskij','name':'Хамовнический районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/horoshevskij','name':'Хорошёвский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/cheryomushkinskij','name':'Черёмушкинский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/chertanovskij','name':'Чертановский районный суд'},
{'url':'https://www.mos-gorsud.ru/rs/shcherbinskij','name':'Щербинский районный суд'}
]

# r = req.get(URL)
# print(r)

hearingRangeDateFrom = Date.today()
params={
    'hearingRangeDateFrom': hearingRangeDateFrom.strftime('%d.%m.%Y'),
    'page':'333'
    }

# url = r'https://www.mos-gorsud.ru/rs/babushkinskij/hearing?hearingRangeDateFrom=22.05.2024&page=377'
url = r'https://www.mos-gorsud.ru/rs/babushkinskij' # &pade=377

   

def get_html(url, params=''):
    full_url = url + '/hearing'
    r = req.get(full_url, headers=HEADERS, params=params)
    if r.status_code == 200:
        return r
    else:
        return None        

def clean(text):
    t =  ct.clean(text, extra_spaces=True, reg=r'[\t\r\n]', reg_replace='')
    return t

def get_content(html):
    soup = BeautifulSoup(html, 'html.parser')    

    table = soup.find_all('div', class_='wrapper-search-tables')
    
    tbody = soup.table('tbody')
    rows = soup.tbody('tr')
 
    cases = []
    for row in rows:
       cells = row.find_all('td')
       cases.append(
            {
                r'DateTime': hearingRangeDateFrom.strftime("%Y-%m-%d"),
                r'CourtName': 'Бабушкинский районный суд',
                r'Number': clean(cells[0].text),
                r'Sides' : clean(cells[1].text),
                r'State' : clean(cells[2].text),
                r'ReviewDateTime' : clean(cells[3].text),
                r'Courtroom' : clean(cells[4].text),
                r'Stage' : clean(cells[5].text),
                r'Judge' : clean(cells[6].text),
                r'List' : clean(cells[7].text)
            }
        )           

    return cases

def save_data(cases):
    try:
        # Create a connection
        connection = pymssql.connect(server=f'{SERVER}', database=f'{DATABASE}')
        cursor = connection.cursor(as_dict=False)

        for item in cases:
            # Fill parameters

            # Call procedure
            cursor.callproc('dbo.InsertReviewData',( 
            item['DateTime'],
            item['CourtName'],
            item['Number'],
            item['Sides'],
            item['State'],
            item['ReviewDateTime'],
            item['Courtroom'],
            item['Stage'],
            item['Judge'],
            item['List']           
            ))

        # Close cursor and connection
        cursor.close()
        connection.commit()
        connection.close()

    except pymssql.Error as ex:
        print("An error occurred in SQL Server:", ex)
        connection.rollback()
    finally:
        # Close the connection
        if 'connection' in locals():
            connection.close()  


html = get_html(url, params=params)
if html != None:
    content = get_content(html.text)
    # print(len(content))
    # print(content)
    save_data(content)
else:
    print('Cannot load page!')


# def parser(full_url):
#     try:
#         html = get_html(full_url)
#         if html.status_code == 200:
#             pass
#         elif html.status_code == 404:
#             return None
#         else:
#             raise TypeError('Error load pages!')
   
#     except pymssql.Error as ex:
#         # print("An error occurred in SQL Server:", ex)
#         connection.rollback()
#     finally:
#         # Close the connection
#         # if 'connection' in locals():
#         #     connection.close()          
        
