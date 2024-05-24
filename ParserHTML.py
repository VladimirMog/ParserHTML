from pymssql import Date
import requests as req
from bs4 import BeautifulSoup
import pymssql
import pandas as pd
import cleantext as ct

############    constants   #####################
SERVER = r'DELL-T7810\SQL2019'
DATABASE = 'Courts'
HEADERS = {
    'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0'
    }
DATE_FROM = Date.today()
COURTS = [
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

def clean(text):
    t =  ct.clean(text, extra_spaces=True, reg=r'[\t\r\n]', reg_replace='')
    return t

def save_data(reviews):
    try:
        # Create a connection
        connection = pymssql.connect(server=f'{SERVER}', database=f'{DATABASE}')
        cursor = connection.cursor(as_dict=False)

        for item in reviews:
            if len(item) == 11:
            # Call procedure
                cursor.callproc('dbo.InsertReviewData',( 
                item['DateTime'],
                item['CourtName'],
                item['Page'],
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

def prepare_parameters(date, page):
    if page == 0:
        parameters = {
            'hearingRangeDateFrom': date.strftime('%d.%m.%Y')
            }  
    else:
        parameters = {
            'hearingRangeDateFrom': date.strftime('%d.%m.%Y'),
            'page': page    
            }
    return parameters

def get_html(url, params=''):
    full_url = url + '/hearing'
    r = req.get(full_url, headers=HEADERS, params=params)
    return r

def get_content(html, court_name, page):
    page_count = 1
    reviews = [] 
    
    if html.status_code == 200:
        soup = BeautifulSoup(html.text, 'html.parser')  
        if page == 0:
            # pages = soup.find("input", {"id": "paginationFormMaxPages"}) #<input type="hidden" id="paginationFormMaxPages" value="372">
            pages = soup.find(id="paginationFormMaxPages") #<input type="hidden" id="paginationFormMaxPages" value="372">
            page_count = pages.get('value')
            reviews.append({r'page_count': page_count})
        else:
            table = soup.find_all('div', class_='wrapper-search-tables')
    
            tbody = soup.table('tbody')
            rows = soup.tbody('tr')
 
            for row in rows:
               cells = row.find_all('td')
               reviews.append(
                    {
                        r'DateTime': DATE_FROM.strftime("%Y-%m-%d"),
                        r'CourtName': court_name,
                        r'Page': page,
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

    return reviews

############    start parsing   ###############
for court in COURTS:
    pages = 1
    url = court['url']
    court_name = court['name']
    params = prepare_parameters(DATE_FROM, 0)
    html = get_html(url, params=params)
    reviews  = get_content(html, court_name, 0)
    if len(reviews) == 1:
        page_count = int(reviews[0]['page_count'])
    
    for page in range(1, page_count):
        params = prepare_parameters(DATE_FROM, page)
        html = get_html(url, params=params)
        reviews = get_content(html, court_name, page)
        if reviews is not None:
            save_data(reviews)
            