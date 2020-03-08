import datetime
import json
import re
from lxml import etree
import requests
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import os
import hashlib
import urllib
from urllib import parse

"""
数据库的表字段
# 基金类型 -- gp_fund_type
# 基金来源名称 -- gp_from_type_name
# 基金代号 -- gp_from_type_daihao
# 年份 -- gp_year
# 季度 -- gp_quarter
# 序号 -- gp_nums
# 股票代码 -- gp_demo
# 股票名称 -- gp_name
# 最新价 -- gp_new_price
# 跌涨幅 -- gp_fall_rise
# 相关资讯 -- gp_information
# 占净值比例 -- gp_value_ratio
# 持股数 -- gp_shares_held
# 持仓市值 -- gp_market_value

创建表的sql语句
create table `shipment_space` (
	`id` double ,
	`gp_fund_type` varchar (300),
	`gp_from_type_name` varchar (900),
	`gp_from_type_daihao` varchar (300),
	`gp_year` varchar (300),
	`gp_quarter` double ,
	`gp_nums` varchar (300),
	`gp_demo` varchar (300),
	`gp_name` varchar (300),
	`gp_new_price` varchar (300),
	`gp_fall_rise` varchar (300),
	`gp_information` varchar (900),
	`gp_value_ratio` varchar (300),
	`gp_shares_held` varchar (300),
	`gp_market_value` varchar (300)
); 


:return

"""


class FundSpider(object):
    def __init__(self, base_path, filename):
        """初始化信息"""
        "http://fund.eastmoney.com/data/fundranking.html#tgp;c0;r;szzf;pn50;ddesc;qsd20190205;qed20200205;qdii;zq;gg;gzbd;gzfs;bbzt;sfbb"
        # 请求头
        self.headers = {
            'Host': 'fund.eastmoney.com',
            'Referer': 'http://fund.eastmoney.com/data/fundranking.html',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36',
        }

        # 文件路径拼接
        self.file_path = base_path + filename

    def filter_url(self, old_url):
        """去重过滤"""

        if not os.path.exists(self.file_path):
            f = open(self.file_path, "wb")
            f.write("[]".encode("utf-8"))
            f.close()
        try:
            with open(self.file_path, "rb") as f:
                content = f.read()
        except Exception as e:
            with open("error_log.txt", "a+") as f:
                f.write(str(e) + "\r\n")
            content = ""

        if content:
            m = hashlib.md5()

            m.update(old_url.encode("utf-8"))
            md5_str = m.hexdigest()

            content = content.decode("utf-8")

            md5_list = json.loads(content)
            md5_set_data = set(md5_list)
            if md5_str in md5_list:
                print(old_url, "---已爬取过")
            else:
                md5_set_data.add(md5_str)
                md5_list = list(md5_set_data)
                md5_str_data = json.dumps(md5_list)

                return [True, md5_str_data]

    def send_request(self, url, params=None):
        """发送请求"""
        try:

            response = requests.get(url, params=params, headers=self.headers)
        except Exception as e:
            with open("error_log.txt", "a+") as f:
                f.write(str(e) + "\r\n")

            response = ""
        return response

    def parse_data(self, response):
        """解析响应"""
        all_fund_message_list = []
        if response:

            response_content = response.content.decode("utf-8")

            # 匹配筛选需要的文本格式
            pattern = re.compile(r'var rankData = {datas:(.*?),allRecords.*?$', re.S)

            # 一个页面中需要的所有数据
            all_fund_list_data = pattern.findall(response_content)[0]
            if all_fund_list_data:

                # 数据格式转换
                list_data = json.loads(all_fund_list_data)

                # 遍历出每一个基金公司的信息
                for fund_message_str in list_data:
                    fund_message_list = fund_message_str.split(",")

                    all_fund_message_list.append(fund_message_list)
                return all_fund_message_list
            else:
                return False

    def save_data(self, co_list, md5_str_data, sql_user, sql_passwd, sql_host, sql_database):
        """保存到数据库"""

        # engine = create_engine('mysql+pymysql://root:@localhost/test1?charset=utf8')
        # engine = create_engine('mysql+pymysql://' + sql_user + ':' + sql_passwd + '@' + sql_host + '/' + sql_database + '?charset=utf8')
        engine = create_engine('mysql+mysqlconnector://' + sql_user + ':' + sql_passwd + '@' + sql_host + '/' + sql_database + '?charset=utf8')

        # 季度 -- gp_quarter
        # 序号 -- gp_nums
        # 股票代码 -- gp_demo
        # 股票名称 -- gp_name
        # 最新价 -- gp_new_price
        # 跌涨幅 -- gp_fall_rise
        # 相关资讯 -- gp_information
        # 占净值比例 -- gp_value_ratio
        # 持股数 -- gp_shares_held
        # 持仓市值 -- gp_market_value
        df_data1 = pd.DataFrame(np.array(co_list),
                                columns=[
                                    "gp_fund_type",
                                    "gp_from_type_name",
                                    "gp_from_type_daihao",
                                    "gp_year",
                                    "gp_quarter",
                                    "gp_nums",
                                    "gp_demo",
                                    "gp_name",
                                    "gp_new_price",
                                    "gp_fall_rise",
                                    "gp_information",
                                    "gp_value_ratio",
                                    "gp_shares_held",
                                    "gp_market_value"
                                ])
        # 将数据写入sql
        # df_data1 = pd.DataFrame(df_data1)
        pd.io.sql.to_sql(df_data1, 'shipment_space', con=engine, if_exists='append', index=False)

        try:
            with open(self.file_path, "w") as f:
                f.write(md5_str_data)
        except Exception as e:
            with open("error_log.txt", "a+") as f:
                f.write(str(e) + "\r\n")

    def get_gp_fall_rise_and_new_price(self, secides):

        url = "https://push2.eastmoney.com/api/qt/ulist.np/get"

        params = {
            'fltt': '2',
            'invt': '2',
            'fields': 'f2,f3,f12,f14,f9',
            'cb': 'jQuery183017426360884884806_1579916507130',
            'ut': '267f9ad526dbe6b0262ab19316f5a25b',
            # 'secids': '1.600276,0.300003,0.000661,0.300482,0.300760,0.002821,1.603456,0.300463,0.002410,1.603127,',
            'secids': secides,
            '_': '1579916507377',
        }
        headers = {
            # 'Accept': '*/*',
            # 'Accept-Encoding': 'gzip, deflate, br',
            # 'Accept-Language': 'zh-CN,zh;q=0.9',
            # 'Connection': 'keep-alive',
            # 'Cookie': 'st_si=50444683357816; st_asi=delete; EMFUND9=02-24%2009%3A17%3A21@%23%24%u534E%u590F%u6210%u957F%u6DF7%u5408@%23%24000001; EMFUND7=01-25 09:41:31@#$%u5DE5%u94F6%u517B%u8001%u4EA7%u4E1A%u80A1%u7968@%23%24001171; st_pvi=70850937535773; st_sp=2020-02-06%2019%3A04%3A18; st_inirUrl=http%3A%2F%2Ffund.eastmoney.com%2Fdata%2Ffundranking.html; st_sn=3; st_psi=20200125094131256-0-1330660859; qgqp_b_id=b9a9464d0b1d5c9ace7b2b12d95629f3; EMFUND1=null; EMFUND2=null; EMFUND3=null; EMFUND4=null; EMFUND5=null; EMFUND0=null; EMFUND6=02-13%2007%3A41%3A15@%23%24%u6C47%u4E30%u664B%u4FE1%u4F4E%u78B3%u5148%u950B%u80A1%u7968@%23%24540008; EMFUND8=02-24%2009%3A01%3A03@%23%24%u4FE1%u8FBE%u6FB3%u94F6%u5148%u8FDB%u667A%u9020%u80A1%u7968%u578B@%23%24006257; cowCookie=true; intellpositionL=585.594px; intellpositionT=671px',
            # 'Host': 'push2.eastmoney.com',
            # 'Referer': 'http://fundf10.eastmoney.com/ccmx_001171.html',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36',
        }

        response = requests.get(url=url, headers=headers, params=params)
        content = response.content.decode("utf-8")
        pattern = re.compile(r'jQuery\d+_\d+\((.*?)\);')
        json_str = pattern.findall(content)

        if json_str:
            json_str = json_str[0]
            dict_data = json.loads(json_str)
            dict_json_0 = dict_data["data"]
            if dict_json_0:
                dict_list = dict_json_0["diff"]
                if dict_list:
                    return dict_list

    def detail_by_xpath_get_data(self, detail_response, year, detail_node, new_year):
        detail_content = detail_response.content.decode("utf-8")

        if detail_content:

            # 匹配筛选需要的文本格式
            pattern = re.compile(r'var apidata={ content:"(.*?)",arryear:.*?$', re.S)
            data = pattern.findall(detail_content)

            if data[0]:

                html = data[0]
                html_obj = etree.HTML(html)

                # 包含所有季度的节点
                nodes_lists = html_obj.xpath('//div[@class="box"]')
                try:

                    secids = html_obj.xpath('//div[@id="gpdmList"]/text()')[0]
                except:
                    secids = ""

                co_list = []

                # 遍历每个季度
                for index, nodes in enumerate(nodes_lists):

                    # 找到单个季度的10个所有股票列表
                    node_list = nodes.xpath(
                        './/div[@class="boxitem w790"]/table/tbody/tr')
                    # 第四季度的
                    if index == quarter and year == new_year:
                        dict_list = self.get_gp_fall_rise_and_new_price(secids)

                        for node in node_list:

                            # 基金类型
                            gp_fund_type = fund_type
                            # 基金来源名称
                            gp_from_type_name = detail_node[1]

                            # 基金代号
                            gp_from_type_daihao = detail_node[2]

                            # 年份
                            gp_year = year

                            # 包含了最新价， 涨跌幅
                            # one_fall_rise_new_price_dict = node_tuple[1]

                            # 基金类型

                            # 季度
                            gp_quarter = str(4 - index)
                            # 序号

                            gp_nums = node.xpath('./td[1]/text()')[0]

                            # 股票代码
                            gp_demo = node.xpath('./td[2]/a/text()')
                            if gp_demo:
                                gp_demo = gp_demo[0]

                            else:
                                try:
                                    gp_demo = node.xpath('./td[2]//text()')[0]
                                except:
                                    gp_demo = ""

                            # 股票名称
                            gp_name = node.xpath('./td[3]/a/text()')
                            if gp_name:
                                gp_name = gp_name[0]

                            else:
                                try:
                                    gp_name = node.xpath('./td[3]//text()')[0]
                                except:
                                    gp_name = ""

                            gp_new_price = ""
                            gp_fall_rise = ""
                            if dict_list:

                                for one_fall_rise_new_price_dict in dict_list:
                                    new_name = one_fall_rise_new_price_dict["f14"]
                                    new_gp_demo = one_fall_rise_new_price_dict["f12"]
                                    if gp_name == new_name or gp_demo == new_gp_demo:
                                        # 最新价
                                        gp_new_price = str(one_fall_rise_new_price_dict["f2"])

                                        # 跌涨幅
                                        gp_fall_rise = str(one_fall_rise_new_price_dict["f3"])

                            # 相关资讯
                            gp_information = node.xpath('./td[6]/a/@href')

                            if gp_information:
                                gp_information = json.dumps(gp_information)
                            else:
                                gp_information = ''

                            # 占净值比例
                            gp_value_ratio = node.xpath('./td[7]/text()')
                            if gp_value_ratio:
                                gp_value_ratio = gp_value_ratio[0]
                                if "%" in gp_value_ratio:
                                    gp_value_ratio = gp_value_ratio.replace("%", "")
                            else:
                                gp_value_ratio = ""
                            # 持股数
                            gp_shares_held = node.xpath('./td[8]/text()')
                            if gp_shares_held:
                                gp_shares_held = gp_shares_held[0]
                            else:
                                gp_shares_held = ""
                            # 持仓市值
                            gp_market_value = node.xpath('./td[9]/text()')
                            if gp_market_value:
                                gp_market_value = gp_market_value[0]
                            else:
                                gp_market_value = ""
                            """
                            # 基金类型 -- gp_fund_type
                            # 基金来源名称 -- gp_from_type_name
                            # 基金代号 -- gp_from_type_daihao
                            # 年份 -- gp_year
                            # 季度 -- gp_quarter
                            # 序号 -- gp_nums
                            # 股票代码 -- gp_demo
                            # 股票名称 -- gp_name
                            # 最新价 -- gp_new_price
                            # 跌涨幅 -- gp_fall_rise
                            # 相关资讯 -- gp_information
                            # 占净值比例 -- gp_value_ratio
                            # 持股数 -- gp_shares_held
                            # 持仓市值 -- gp_market_value
                            """
                            data_row = [
                                gp_fund_type,
                                gp_from_type_name,
                                gp_from_type_daihao,
                                gp_year,
                                gp_quarter,
                                gp_nums,
                                gp_demo,
                                gp_name,
                                gp_new_price,
                                gp_fall_rise,
                                gp_information,
                                gp_value_ratio,
                                gp_shares_held,
                                gp_market_value]
                            co_list.append(data_row)


                    else:

                        for node in node_list:

                            # 基金类型
                            gp_fund_type = fund_type
                            # 基金来源名称
                            gp_from_type_name = detail_node[1]

                            # 基金代号
                            gp_from_type_daihao = detail_node[2]

                            # 年份
                            gp_year = year

                            # 季度
                            gp_quarter = str(4 - index)
                            # 序号
                            gp_nums = node.xpath('./td[1]/text()')
                            if gp_nums:
                                gp_nums = gp_nums[0]
                            else:
                                gp_nums = ""

                            # 股票代码
                            gp_demo = node.xpath('./td[2]//text()')
                            if gp_demo:
                                gp_demo = gp_demo[0]
                            else:

                                gp_demo = ""

                            # 股票名称
                            gp_name = node.xpath('./td[3]//text()')
                            if gp_name:
                                gp_name = gp_name[0]
                            else:
                                gp_name = ""

                            # 最新价
                            gp_new_price = ""

                            # 跌涨幅
                            gp_fall_rise = ""

                            # 相关资讯
                            gp_information = node.xpath('./td[4]/a/@href')
                            if gp_information:
                                gp_information = json.dumps(gp_information)
                            else:
                                gp_information = ""
                            # 占净值比例
                            gp_value_ratio = node.xpath('./td[5]/text()')
                            if gp_value_ratio:
                                gp_value_ratio = gp_value_ratio[0]
                                if "%" in gp_value_ratio:
                                    gp_value_ratio = gp_value_ratio.replace("%", "")

                            else:
                                gp_value_ratio = ''

                            # 持股数
                            gp_shares_held = node.xpath('./td[6]/text()')
                            if gp_shares_held:
                                gp_shares_held = gp_shares_held[0]
                            else:
                                gp_shares_held = ''

                            # 持仓市值
                            gp_market_value = node.xpath('./td[7]/text()')
                            if gp_market_value:
                                gp_market_value = gp_market_value[0]
                            else:
                                gp_market_value = ''

                            # 基金类型 -- gp_fund_type
                            # 基金来源名称 -- gp_from_type_name
                            # 基金代号 -- gp_from_type_daihao
                            # 年份 -- gp_year
                            # 季度 -- gp_quarter
                            # 序号 -- gp_nums
                            # 股票代码 -- gp_demo
                            # 股票名称 -- gp_name
                            # 最新价 -- gp_new_price
                            # 跌涨幅 -- gp_fall_rise
                            # 相关资讯 -- gp_information
                            # 占净值比例 -- gp_value_ratio
                            # 持股数 -- gp_shares_held
                            # 持仓市值 -- gp_market_value
                            data_row = [
                                gp_fund_type,
                                gp_from_type_name,
                                gp_from_type_daihao,
                                gp_year,
                                gp_quarter,
                                gp_nums,
                                gp_demo,
                                gp_name,
                                gp_new_price,
                                gp_fall_rise,
                                gp_information,
                                gp_value_ratio,
                                gp_shares_held,
                                gp_market_value]
                            co_list.append(data_row)
                        pass

                return co_list

    def main(self, begin, end, fund_type, base_path, sql_user, sql_passwd, sql_host, sql_database, all_years, quarter,
             new_year):
        """函数的主入口"""
        url = """http://fund.eastmoney.com/data/rankhandler.aspx?"""
        page = 1
        nums = 1
        params = {
            'op': 'ph',
            'dt': 'kf',
            # 基金类型
            'ft': fund_type,
            'rs': '',
            'gs': '0',
            # 排序方式
            'sc': 'zzf',
            # 升序、降序
            'st': 'desc',
            # 开始日期
            'sd': begin,
            # 结束日期
            'ed': end,
            'qdii': '',
            'tabSubtype': ',,,,,',
            # 页码
            'pi': str(page),
            'pn': '10000',
            'dx': '1',
            'v': '0.05992793447254674',
        }

        data = urllib.parse.urlencode(params)
        print("正在爬取--urlurl", url + data)
        #
        # url_and_md5 = self.filter_url(old_url=url + data)
        #
        # try:
        #     is_new_url = url_and_md5[0]
        # except:
        #     is_new_url = False
        is_new_url = True
        if is_new_url:
            response = self.send_request(url, params)
            fund_message_list = self.parse_data(response)

            # 返回有页面内容
            if fund_message_list:

                for detail_node in fund_message_list:

                    detail_url = "http://fundf10.eastmoney.com/FundArchivesDatas.aspx?"
                    detail_headers = {
                        'Host': 'fundf10.eastmoney.com',
                        'Referer': "http://fundf10.eastmoney.com/ccmx_" + detail_node[0] + ".html",
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36', }
                    for year in all_years:
                        detail_params = {
                            'type': 'jjcc',
                            'code': detail_node[0],
                            'topline': '10',
                            'year': year,
                            'month': '',
                            'rt': '0.5759138029336275',
                        }

                        url_and_md5 = self.filter_url(old_url=detail_url + urllib.parse.urlencode(detail_params))
                        try:
                            is_detail_url = url_and_md5[0]
                        except Exception as e:
                            with open("error_log.txt", "a+") as f:
                                f.write(str(e) + "\r\n")
                            is_detail_url = False
                        if is_detail_url:

                            detail_response = requests.get(detail_url, headers=detail_headers, params=detail_params)
                            co_list = self.detail_by_xpath_get_data(detail_response, year, detail_node, new_year)
                            if co_list:

                                print("%s当前url正存入数据库》》:%s" %(nums, detail_url + urllib.parse.urlencode(detail_params)))
                                self.save_data(co_list, url_and_md5[1], sql_user, sql_passwd, sql_host,
                                               sql_database)
                                nums += 1
                            # else:
                            #     return "url%s没有数据" % detail_response.url

            else:
                return "返回的data是空的"

            # if page == 3:
            #     break


if __name__ == '__main__':
    # 文件名
    # filename = "filter_set.txt"

    # 详情页面文件名
    filename = "detail_sets.txt"
    # 目录
    base_path = r"C:\Users\SVUS\Desktop\demo/"

    # mysql数据库参数
    # 用户名
    sql_user = "root"
    # 密码
    sql_passwd = ""
    # ip
    sql_host = "localhost"
    # 数据库名
    sql_database = "test1"

    # 持仓信息中的年份
    all_years = ["2019"]

    # -------------以下参数:quarter, new_year 是页面中显示的，有最新涨跌幅数据的季度， 年份-----------
    # 季度
    # 0--第4季度
    # 1--第3季度
    # 2--第2季度
    # 3--第1季度
    quarter = 0

    # 年份
    new_year = "2019"
    # -------------以上参数:quarter, new_year 是页面中显示的，有最新涨跌幅数据的季度， 年份----------





    # 创建对象
    spider = FundSpider(base_path, filename)

    # 自定义的开始日期
    begin = datetime.date(2020, 2, 5)
    # 结束日期
    end = datetime.date(2020, 2, 5)

    # 将基金类型进行遍历，依次抓取
    # "gp" 股票型
    # "hh" 混合型
    # "zq" 债券型
    # "zs" 指数型
    # "bb" 保本型
    # "qdii" QDII
    # "lof" LOF
    # "fof" FOF
    for fund_type in ["gp", "hh", "zq", "zs", "bb", "qdii", "lof", "fof"]:
        print(spider.main(begin, end, fund_type, base_path, sql_user, sql_passwd, sql_host, sql_database, all_years,
                          quarter, new_year))


