# -*- coding: utf-8 -*-
''' Python Spider grab the exchange data '''
from selenium import webdriver
from msedge.selenium_tools import EdgeOptions
from msedge.selenium_tools import Edge
from bs4 import BeautifulSoup

import time
import datetime
# import holidays
import re
import cx_Oracle

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


### Setting the oracle database connection
def oracle_connection():

    host = "localhost"
    port = "1521"
    service_name = "orcl"
    user = "root"
    password = "12345"
    database = "test"

    try:
        dsn = cx_Oracle.makedsn(host, port, service_name=service_name, sid=database)
        connection = cx_Oracle.connect(user, password, dsn)
        cursor = connection.cursor()
        print("Successfully connected to the Oracle database!")
        return cursor, connection

    except cx_Oracle.DatabaseError:
        print("Unable to connect to the database")
        return False


### check today's date is in database or not
def check_date_in_db(date):

    cursor, conn = oracle_connection()
    sql = 'SELECT DISTINCT \'datetime\' FROM "forexExchange"'

    cursor.execute(sql)
    results = cursor.fetchall()
    datestr = [i[0] for i in results]
    
    return str(date) not in datestr

### check the href is in database or not
def check_href_in_db(href):

    cursor, conn = oracle_connection()
    sql = 'SELECT DISTINCT \'url\' FROM "forexExchange"'

    cursor.execute(sql)
    results = cursor.fetchall()
    hreflist = [s[0] for s in results]

    return str(href) not in hreflist
    

### get the url about exchange notice webpage
def get_html_url(options, url):
    driver = Edge(options=options,executable_path='D:\\python2.7.18\\python2.7.18\\msedgedriver.exe')
    driver.get(url)
    html_content = driver.page_source
    soup = BeautifulSoup(html_content, 'html.parser')
    atags = soup.select('a')

    anew = None
    for atag in atags:
        if atag.get_text().decode('utf-8').endswith('中国外汇交易中心受权公布人民币汇率中间价公告'):
            # print(f"{tag.get_text()}: http://www.pbc.gov.cn{tag.get('href')}")
            anew = 'http://www.pbc.gov.cn' + atag.get('href')
            break
    driver.quit()

    return str(anew)


### check if the url has been updated
def check_update(old_content, new_content):
    return old_content != new_content


### get the foreign exchange information
def data_obtain(driver, url):
    driver.get(url)
    url_source = driver.page_source

    soup = BeautifulSoup(url_source, 'html.parser')
    p_tags = soup.select('p')

    notice = ""
    for ptag in p_tags:
        if ptag.get_text().startswith('中国人民银行授权中国外汇交易中心公布'):
            notice = ptag.get_text()

    sentences = []
    lenSentence = []
    for sentence in notice.split("，"):
        if '人民币' in sentence:
            lenSentence.append(len(sentence))
            if len(sentence) > 19:
                s = sentence[-14:-1] + '元'
                sentences.append(str(s))
            else:
                sentences.append(str(sentence))
    
    c_to_r = []
    r_to_c = []
    for i in sentences:
        if i[0].isdigit():
            c_to_r.append(i)
        else:
            r_to_c.append(i)

    print "外币对人民币: \n", c_to_r
    print "人民币对外币: \n", r_to_c
    return c_to_r, r_to_c


### process the information and save it
def process_save(list1, list2, date, href):

    today = date.strftime('%Y-%m-%d')
    cursor, conn = oracle_connection()

    ### define the data insertion SQL
    insert_sql = 'INSERT INTO "forexExchange" VALUES (:1, :2, :3, :4, :5)'
    # (\'datetime\', \'forex_rate\', \'currency\', \'rmb_rate\', \'url\')
    # insert_sql = "INSERT INTO FOREX_EXCHANGE ('date', 'forex_rate', 'currency', 'rmb_rate', 'url') VALUES (%s, %s, %s, %s, %s)"

    for c in list1:
        c = str(c)
        matchn = re.search(r'([\w\s\S]+)*对\s*人民币(\d+.\d+)', c)

        c_value = matchn.group(1)
        r_value = "{:.4f}".format(float(matchn.group(2)))
        v = re.findall(r'\d+|[^\d\s]+', c_value)
        # print 'c_value: ', c_value, 'r_value: ', r_value

        # if v[1] == '日元':
        #     v[0] = int(v[0]) / 100
        #     r_value = round(r_value / 100, 4)

        print v[1], ':', v[0], ':', r_value

        currency = v[1].decode("utf-8").encode("GB2312")

        try:
            # print today, v[0], v[1], r_value, href
            cursor.execute(insert_sql, (today, str(v[0]), currency, str(r_value), str(href)))
            conn.commit()
            print "New Forex data insert into database succeed!"
        except cx_Oracle.DatabaseError as e:
            print "Error inserting data: ", e


    for r in list2:
        # print r
        matchr = re.search(r'人民币\s*(\d+)元对\s*([\d.]+)\s*([\w\s\S]+)', str(r))
        # print type(matchr)

        r1 = matchr.group(1)
        c1 = "{:.4f}".format(float(matchr.group(2)))
        cname = matchr.group(3)

        # print type(cname), type(r1), type(c1)
        print cname, ':', c1, ':', r1

        if str(cname) == '泰铢。':
            cname = cname.rstrip('。')

        cname = cname.decode("utf-8").encode("GB2312")

        try:
            cursor.execute(insert_sql, (today, str(c1), cname, str(r1), str(href)))
            conn.commit()
            print('New Forex data insert into database succeed!')
        except cx_Oracle.DatabaseError as e:
            ### output the error information
            print 'Error inserting data: ', e

    ### close the connection of database
    cursor.close()
    conn.close()

    ### write the exchange information into txt file
    # with open('output.txt', 'a') as f:
    #     f.write('---------------------------\n')
    #     f.write(f'{today}: \n')
    #     for item in data:
    #         f.write(f'{item}\n')
    #     f.write('---------------------------\n')
    return


### main function
def main():
    ### set the headless option to the edge
    options = EdgeOptions()
    options.add_argument('--headless')

    url = "http://www.pbc.gov.cn/zhengcehuobisi/125207/125217/125925/index.html"
    old_content = ""

    while True:

        ### get the local time
        check_time = time.strftime("%H:%M", time.localtime())
        ### get the date today
        today = datetime.date.today()

        if today.weekday() < 5 and check_date_in_db(today):
            if check_time >= "09:20" and check_time <= "10:10":

                ### catch the content of html
                new_content = get_html_url(options, url)

                ### check the update
                if check_update(old_content, new_content) and check_href_in_db(new_content):
                    print("Webpage already updated! Great~")
                    old_content = new_content
                    print today, ' ', check_time, ':', new_content

                    driver = Edge(options=options, executable_path='D:\\python2.7.18\\python2.7.18\\msedgedriver.exe')

                    c_to_r, r_to_c = data_obtain(driver, new_content)
                    process_save(c_to_r, r_to_c, today, new_content)
                    
                    driver.quit()

                    print('Forex Information crawled successfully, Waiting for the next update......')
                    time.sleep(5)

                else:
                    print('Waiting for the webpage updating......')
                    time.sleep(120)
            else:
                print('system sleeping...')
                time.sleep(600)
        else:
            print("Oh, System not in working time / todays' date already in DB")
            time.sleep(1200)

if __name__ == '__main__':
    main()
    