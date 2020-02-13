from master import KumoMaster

process1 = lambda soup: [i.find('a')['href'] for i in soup.find_all('div', class_ = "info clear")]


"""
Basic information for apartment:
房屋户型
所在楼层
建筑面积
户型结构
套内面积
建筑类型
房屋朝向
建筑结构
装修情况
梯户比例
配备电梯
产权年限

Basic information for villa:
房屋户型
所在楼层
建筑面积
套内面积
房屋朝向
建筑结构
装修情况
别墅类型
产权年限
"""

def process2(soup):
    base = soup.find("div", class_ = "base").find_all("li") #  the basic information 
    detail = [
        'fzxu_villa' if len(base) == 9 else 'fzxu_apartment',
        soup.find("span", class_ = "total").text,  # the price
        soup.find("span", class_ = "unitPriceValue").text,  # the unit price
        soup.find("div", class_ = "communityName").find("a", class_ = "info").text,  # community
        soup.find("div", class_="areaName").find("span", class_="info").text,  # location
        soup.find("div", class_="areaName").find("a", class_="supplement").text,  # the subway
    ] + [i.text[4:] for i in base]
    return ','.join([str(d) for d in detail])

def get_num_page(soup):
    page_data = soup.find("div", class_ = "page-box house-lst-page-box")["page-data"]
    return str(eval(page_data)["totalPage"])

def main():
    areas = ['https://sz.lianjia.com/ershoufang/luohuqu/',
            'https://sz.lianjia.com/ershoufang/futianqu/',
            'https://sz.lianjia.com/ershoufang/nanshanqu/',
            'https://sz.lianjia.com/ershoufang/yantianqu/',
            'https://sz.lianjia.com/ershoufang/baoanqu/',
            'https://sz.lianjia.com/ershoufang/longgangqu/',
            'https://sz.lianjia.com/ershoufang/longhuaqu/',
            'https://sz.lianjia.com/ershoufang/guangmingqu/',
            'https://sz.lianjia.com/ershoufang/pingshanqu/',
            'https://sz.lianjia.com/ershoufang/dapengxinqu/']
    targets = []
    for a in areas:
        for i in range(1, 8):
            if a.endswith('longgangqu/') and i == 3:
                targets.append(a + 'l1l2l3p{}/'.format(i))
                targets.append(a + 'l4l5l6p{}/'.format(i))
            else:
                targets.append(a + 'p{}/'.format(i))
    KD = KumoMaster(5, targets, get_num_page, 'get_num_page', 7, output='page_nums.csv')
    KD.run()
    del KD

    # get all pages
    targets = []
    for l in open('page_nums.csv').readlines():
        area, pn = l.strip().split(',')
        for i in range(1, int(pn)+1):
            parts = area.split('/')
            parts[-2] = 'pg{}'.format(i) + parts[-2]
            targets.append('/'.join(parts))
    KD = KumoMaster(10, targets, process1, 'process1', 20, output='each_page.csv', flat=True)
    KD.run()
    del KD

    # get details
    targets = list(set([l.strip().split(',')[1] for l in open('each_page.csv').readlines()]))
    KD = KumoMaster(10, targets, process2, 'process2', 20, output='detail.csv')
    KD.run()


if __name__ == '__main__':
    main()
