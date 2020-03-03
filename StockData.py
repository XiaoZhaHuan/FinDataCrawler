import baostock as bs
import pandas as pd
import tushare as ts
import datetime
import time
import sys 
import pymysql
import mpl_finance as mpf
import matplotlib.pyplot as plt
from matplotlib.pylab import date2num
%matplotlib inline

# get stock code
def get_name():
    # login
    lg = bs.login()
    # show login info
    print('login respond error_code:' + lg.error_code)
    print('login respond error_msg:' + lg.error_msg)
    # show stock info
    nowdate = datetime.datetime.now()
    delta = datetime.timedelta(days = -4) # today is Monday
    today = nowdate + delta
    today = today.strftime('%Y-%m-%d')
    rs = bs.query_all_stock(day = today)
    print('query_all_stock respond error_code:' + rs.error_code)
    print('query_all_stock respond error_msg:' + rs.error_msg)
    # show results
    data_list = []
    while(rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list,columns = rs.fields)
    # logout
    bs.logout()
    conn = pymysql.connect(host = "localhost", user = "root", database = "STOCK", password = "password", 
                           use_unicode = True, charset = "utf8")
    cursor1 = conn.cursor()
    ls = []
    for i in range(len(result['code'])):
        ls.append([result['code'][i].split('.')[1], result['tradeStatus'][i], result['code_name'][i]])
    for i in range(len(ls)):
        commit_data = [ls[i][0], ls[i][1].encode('utf-8'), ls[i][2].encode('utf-8'), today]
        cursor1.execute('INSERT INTO stock_code_data values(%s,%s,%s,%s)', commit_data)
    conn.commit()
    conn.close()
    return ls
        
def get_today_stock(name_ls):
    for code in name_ls:
        a = ts.get_today_ticks(code)
        print(a)
    
def get_history_stock(last_date, now_date, name_ls):
    for code in name_ls:
        b = ts.get_hist_data(code, start = last_date, end = now_date)
        print(b)

if __name__ == '__main__':
    name_ls = get_name()
    print(name_ls)
    
    # get stock data
    db = pymysql.connect(host = "localhost", user = "root", database = "STOCK", password = "password", 
                         use_unicode = True, charset = "utf8")
    cursor = db.cursor()
    sql = "SELECT * FROM `stock_code_data` where date = '2020-02-28';"
    cursor.execute(sql)
    data1 = cursor.fetchall()
    data1 = list(data1)
    data = []
    for i in data1:
        data.append([str(i[0]), str(i[1]), str(i[2]), str(i[3])])   
    data1=[]
    for i in data:
        data1.append([i[0],i[3]])
    print(data1)
    db = pymysql.connect(host = "localhost", user = "root", database = "STOCK", password = "password", 
                         use_unicode = True, charset = "utf8")
    cursor = db.cursor()
   
    for j in data1:
        date=datetime.datetime.strptime(j[1],'%Y-%m-%d')
        delta1 = datetime.timedelta(days = -30)
        lastdate = date + delta1
        end = '2020-02-28'
        start = lastdate.strftime('%Y-%m-%d')
        print(start, end)
        code = j[0]
        b = ts.get_hist_data(code, start=start, end=end)
        print(str(type(b)))
        if str(type(b))=="<class 'NoneType'>":
            pass
        else:
            lenth = len(b['close'])
            if lenth > 1:
                for a in range(lenth):
                    inputdata = [code, str(b.index[a]), str(b['open'][a]), str(b['high'][a]), str(b['close'][a]),
                                str(b['low'][a]), str(b['volume'][a]), str(b['price_change'][a]), str(b['p_change'][a]),
                                str(b['ma5'][a]), str(b['ma10'][a]), str(b['ma20'][a]), str(b['v_ma5'][a]),
                                str(b['v_ma10'][a]), str(b['v_ma20'][a]), '0']
                    # print(inputdata)
                    cursor.execute('INSERT INTO stock_day_data values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', inputdata)
                    # print('success')
            db.commit()
    db.close()
    print('success')
    
    # plot K-graph
    start = '2020-02-03'
    end = '2020-02-28'
    k_d = ts.get_k_data('000001',start,end,ktype='D')
    k_d.head()
    k_d.date = k_d.date.map(lambda x: date2num(datetime.datetime.strptime(x,'%Y-%m-%d')))
    quotes = k_d.values

    fig, ax = plt.subplots(figsize=(8,5))
    fig.subplots_adjust(bottom=0.2)
    mpf.candlestick_ochl(ax,quotes,width=0.6,colorup='r',colordown='g',alpha=0.8)
    plt.grid(True)
    ax.xaxis_date()
    ax.autoscale_view()
    plt.setp(plt.gca().get_xticklabels(),rotation=30)
    plt.show()
