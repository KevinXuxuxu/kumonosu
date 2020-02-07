import requests
from bs4 import BeautifulSoup
import pandas as pd
from fake_useragent import UserAgent
import time as t

class LianjiaSpider(object):

    def __init__(self):
        self.headers = {"User-Agent": UserAgent().random}
        self.datas = list()

    def getMaxPage(self, url):
        response = requests.get(url, headers = self.headers)
        if response.status_code == 200:
            source = response.text
            soup = BeautifulSoup(source, "html.parser")
            pageData = soup.find("div", class_ = "page-box house-lst-page-box")["page-data"]
            # pageData = '{"totalPage":100,"curPage":1}'， through the eval () the dictionary () function converts a string to a dictionary 
            maxPage = eval(pageData)["totalPage"]
            return  maxPage
        else:
            print("Fail status: {}".format(response.status_code))
            return None


    def parsePage(self, url):
        maxPage = self.getMaxPage(url)
        #   parse each page to get a link for each secondary home 
        for pageNum in range(1, maxPage+1 ):
            s = t.time()
            url = "https://sz.lianjia.com/ershoufang/pg{}/".format(pageNum)
            print(" currently crawling : {}  ".format(url), end='', flush=True)
            try:
                response = requests.get(url, headers = self.headers)
            except Exception as e:
                print(e)
                continue
            soup = BeautifulSoup(response.text, "html.parser")
            links = soup.find_all("div", class_ = "info clear")
            for i in links:
                link = i.find("a")["href"]    # each <info clear> there are lots of labels <a>, and we only need the first one find
                detail = self.parseDetail(link)
                self.datas.append(detail)
            print("{} sec".format(int(t.time()-s)))

        #   store all the second-hand house data crawled into csv file 
        data = pd.DataFrame(self.datas)
        # columns field ： customize the order of columns （DataFrame by default, sort by dictionary order of column names ）
        columns = [" community ", " door model ", " area ", " the price ", " the unit price ", " toward ", " the elevator ", " location ", " the subway "]
        data.to_csv(".\Lianjia_II.csv", encoding='utf_8_sig', index=False, columns=columns)


    def parseDetail(self, url):
        response = None
        try:
            response = requests.get(url, headers = self.headers)
        except Exception as e:
            print(e)
            return None
        detail = {}
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            detail[" the price "] = soup.find("span", class_ = "total").text
            detail[" the unit price "] = soup.find("span", class_ = "unitPriceValue").text
            detail[" community "] = soup.find("div", class_ = "communityName").find("a", class_ = "info").text
            detail[" location "] = soup.find("div", class_="areaName").find("span", class_="info").text
            detail[" the subway "] = soup.find("div", class_="areaName").find("a", class_="supplement").text
            base = soup.find("div", class_ = "base").find_all("li") #  the basic information 
            detail[" door model "] = base[0].text[4:]
            detail[" area "] = base[2].text[4:]
            detail[" toward "] = base[6].text[4:]
            detail[" the elevator  "] = base[10].text[4:]
            return detail
        else:
            return None

def main():
    url = 'https://sz.lianjia.com/ershoufang/'
    LianjiaSpider().parsePage(url)

if __name__ == '__main__':
    main()
