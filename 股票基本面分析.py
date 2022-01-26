#!/usr/bin/python3
# -*- coding: utf-8 -*-
'''
@Time    : 2022/1/5 0005 9:38
@Author  : youngorc
@FileName: 股票基本面分析.py
@Software: PyCharm
@GitHub:https://github.com/youngorc
代码仅用于爬取东方财富上股票财务相关数据，网页可能存在变更
'''

import requests
import os
import re
import pandas as pd
import json
import math
import xlrd
import datetime
import matplotlib.pyplot as plt
import numpy as np
import argparse
import time
import datetime
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor,as_completed,wait
plt.rcParams['font.family'] = ['SimHei']
plt.rcParams['axes.unicode_minus']=False

pd.set_option('display.max_columns', None)

def get_quote_code(quotes):
    '''爬取股票的名称以及代码'''
    dict1={}
    for name in quotes:
        try:
            url='https://suggest3.sinajs.cn/suggest/type=&key={}'.format(name)
            r=requests.get(url)
            res = r.text
            resultList=re.search('"(.+)"',res).group(1).split(",")
            value=resultList[3].upper()
            key=resultList[4]
            dict1[key]=value
        except:
            pass
    return dict1

def get_business_data(quote,url,dates):
    '''爬取经营分析数据'''
    params = {'code': quote[1]}
    try:
        r=requests.get(url,params=params)
        res=r.text
        res=json.loads(res)
        data=res["zygcfx"]
        df=pd.DataFrame(data)
        dfOutput=df[df["REPORT_DATE"].str[:10].isin(dates)].copy()
        dfOutput.loc[:,"股票"] = [quote[0]]*len(dfOutput)
        print("{} 经营分析数据爬取成功！".format(quote[0]))
    except:
        dfOutput = pd.DataFrame()
        print("{} 经营分析数据爬取失败".format(quote[0]))
    return dfOutput

def get_pettm(quote):
    url="https://push2.eastmoney.com/api/qt/stock/get"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

    }
    params = {
        'secid': '{}.{}'.format("1" if quote[1].startswith("SH") else '0',quote[1][2:])
        , 'fields': 'f164'
        ,'np': '1'
        ,'fltt': '2'
        ,'invt': '2'
        # ,"lmt": '10'
    }
    r = requests.get(url, params=params, headers=headers)
    dict1 = json.loads(r.text)
    dict2 = dict1["data"]
    return dict2["f164"]

def get_zyzb_data(quote,url,key):
    '''爬取主要财务指标数据'''
    params = {'type':0
        , 'code': quote[1]}
    try:
        r=requests.get(url,params=params)
        res=r.text
        res=json.loads(res)
        data=res["data"]
        df=pd.DataFrame(data).transpose()
        df.columns = df.loc["REPORT_DATE"].str[:10]
        dfOutput = df.loc[list(colTables[key].keys())].copy()
        dfOutput.rename(index=colTables[key], inplace=True)
        dfOutput.loc["市盈率ttm",dfOutput.columns[0]]=get_pettm(quote)
        dfOutput.loc[:,"股票"] = [quote[0]]*len(dfOutput)
        print("{} {}数据爬取成功！".format(quote[0],key))
    except:
        dfOutput = pd.DataFrame()
        print("{} {}数据爬取失败".format(quote[0],key))
    return dfOutput

def get_financial_data(quote,url,dates,key):
    '''爬取财务报表数据'''
    params = {'companyType': '4'
        , 'reportDateType': '0'
        , 'reportType': '1'
        , 'dates': ",".join(dates)
        , 'code': quote[1]}
    try:
        r=requests.get(url,params=params)
        res=r.text
        res=json.loads(res)
        data=res["data"]
        df = pd.DataFrame(data).transpose()
        df.columns=df.loc["REPORT_DATE"].str[:10]
        dfOutput = df.loc[list(colTables[key].keys())].copy()
        dfOutput.rename(index=colTables[key],inplace=True)
        dfOutput.loc[:,"股票"] = [quote[0]]*len(dfOutput)
        print("{} {}数据爬取成功！".format(quote[0],key))
    except:
        dfOutput=pd.DataFrame()
        print("{} {}数据爬取失败".format(quote[0],key))
    return dfOutput


def generate_dates(lastperiod=2,times=5,today=datetime.datetime.now()):
    dates=[]
    for i in range(times):
        currentPeriod = (pd.to_datetime(today) - (i+lastperiod) * pd.offsets.QuarterEnd()).strftime("%Y-%m-%d")
        dates.append(currentPeriod)
    return dates

def crawl_all_data(quote,urlTables,lastperiod=2):
    '''按照股票抓取经营分析表，主要指标表、资产负债表、利润表、现金流量表'''
    dates = generate_dates(2)
    result={}
    dffin=pd.DataFrame()
    for key in urlTables.keys():
        if key == "主要指标":
            dfzb=get_zyzb_data(quote,urlTables[key],key)
            # result[key]=get_zyzb_data(quote,urlTables[key],key)
        elif key == "经营分析":
            dfjy=get_business_data(quote,urlTables[key],dates)
            # result[key]=get_business_data(quote,urlTables[key],dates)
        else:
            dffin=dffin.append(get_financial_data(quote,urlTables[key],dates,key))
            # result[key]=get_financial_data(quote,urlTables[key],dates,key)
    return [dfjy,dfzb,dffin]


def load_txt(file):
    with open(file,mode="r",encoding="utf-8") as wp:
        content=wp.read()
        table=json.loads(content)
    return table

def multithread_crawl_data(config):
    '''按照报表批量爬取股票池中的报表数据'''
    with open(config.quotes,mode="r",encoding="utf-8") as wp:
        content=wp.read()
    quotes=get_quote_code(content.split("\n"))
    urlTables=load_txt(config.urlTables)
    dates=generate_dates(2)
    dfjy=pd.DataFrame()
    dfzb=pd.DataFrame()
    dffin=pd.DataFrame()
    writer = pd.ExcelWriter(config.output)
    pool = ThreadPoolExecutor(max_workers=2)
    for key in urlTables.keys():
        tasks=[]
        if key == "经营分析":
            for quote in quotes.items():
                future=pool.submit(get_business_data,(quote),(urlTables[key]),(dates))
                tasks.append(future)
            for future in as_completed(tasks):
                result = future.result()
                dfjy = dfjy.append(result)
                dfjy.to_excel(writer, sheet_name=key,index=None)
        elif key == "主要指标":
            for quote in quotes.items():
                future=pool.submit(get_zyzb_data,(quote),(urlTables[key]),(key))
                tasks.append(future)
            for future in as_completed(tasks):
                result = future.result()
                dfzb = dfzb.append(result)
                dfzb.to_excel(writer, sheet_name=key)
        else:
            for quote in quotes.items():
                future=pool.submit(get_financial_data,(quote),(urlTables[key]),(dates),(key))
                tasks.append(future)
            for future in as_completed(tasks):
                result = future.result()
                dffin = dffin.append(result)
    dffin.to_excel(writer, sheet_name="财务分析")
    writer.save()
    writer.close()
    return [dfjy,dfzb,dffin]

def plot_bar(data,title):
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    fig, axes = plt.subplots(1, 1,figsize=(24,8))
    graph=data.copy()
    graph = graph.reset_index()
    width=0.5
    graph.plot(kind="barh",ax=axes,title=title,width=width)
    axes.set_yticks(graph.index)   # 标注.text方法标注数据需要先把axes的xticks设置号
    axes.set_yticklabels(graph['index'],rotation=0,fontsize=8)
    for idx in graph.index:
        n=len(graph.columns)-1
        mid=n/2-0.5
        for j in range(n):
            axes.text(graph.iloc[:,1+j].loc[idx],idx-(math.ceil(mid-j))*width/n,"{:.2f}".format(graph.iloc[:,1+j].loc[idx]),fontsize=7\
                         ,va="bottom",ha="left",weight='bold')
    plt.tight_layout()
    plt.savefig("./股票{}对比分析情况-{}".format(title,today), bbox_inches='tight', dpi=200)
    plt.close()
































    print("{}绘制完毕!".format(title))

def plot_comp_graph(result):
    '''绘制多个股票的对比柱状图'''
    dfzb=result[1].copy()
    dffin=result[2].copy()
    dfzb.iloc[:,0] = dfzb.iloc[:,0].astype(float)
    dffin.iloc[:, 0] = dffin.iloc[:, 0].astype(float)
    dfzbGraph=pd.pivot_table(dfzb,values=dfzb.columns[0],index=dfzb.index,columns=["股票"])  #主要指标画图数据
    dffintmp=pd.pivot_table(dffin,values=dffin.columns[0],index=dffin.index,columns=["股票"])
    dfzbGraph.loc["现金比率"] = dffintmp.loc["现金"]/dffintmp.loc["流动负债"] * 100
    dffinGraph=dffintmp.apply(lambda x:x/100000000).copy()   #财务指标画图数据
    upGraph = dfzbGraph.loc[['每股收益', '流动比率', '销售毛利率', '存货周转率', '净资产收益率', '现金比率'], :].append(
        dffinGraph.loc[['应付账款', '营业总收入', '研发费用', '净利润', '持续经营净利润'], :])
    downGraph = dfzbGraph.loc[['资产负债率', '市盈率ttm'], :].append(dffinGraph.loc[['应收账款', '存货', '流动负债', '非流动负债'], :])
    upGraph.fillna(0)
    plot_bar(upGraph.fillna(0),"上行指标")
    plot_bar(downGraph.fillna(0), "下行指标")


if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--table2col",default=r"G:\股市分析\报表列名对应.txt")
    parser.add_argument("--urlTables",default=r"G:\股市分析\urlTables.txt")
    parser.add_argument("--quotes", default=r"G:\股市分析\quotes.txt")
    parser.add_argument("--output",default=r"G:\股市分析\股票对比分析.xlsx")
    config = parser.parse_args(args=[])
    colTables = load_txt(config.table2col)
    result=multithread_crawl_data(config)
    plot_comp_graph(result)