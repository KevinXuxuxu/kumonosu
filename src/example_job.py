from master import KumoMaster

process1 = lambda soup: [i.find('a')['href'] for i in soup.find_all('div', class_ = "info clear")]

def process2(soup):
    base = soup.find("div", class_ = "base").find_all("li") #  the basic information 
    detail = [
        soup.find("span", class_ = "total").text,  # the price
        soup.find("span", class_ = "unitPriceValue").text,  # the unit price
        soup.find("div", class_ = "communityName").find("a", class_ = "info").text,  # community
        soup.find("div", class_="areaName").find("span", class_="info").text,  # location
        soup.find("div", class_="areaName").find("a", class_="supplement").text,  # the subway
        base[0].text[4:],  # door model
        base[2].text[4:],  # area
        base[6].text[4:],  # toward
        base[10].text[4:]  # the elevator
    ]
    return ','.join([str(d) for d in detail])

def main():
    # get all pages
    targets = ["https://sz.lianjia.com/ershoufang/pg{}/".format(i) for i in range(1,11)]
    KD = KumoMaster(3, targets, process1, 'process1', 3, output='each_page.csv', flat=True)
    KD.run()
    del KD

    # get details
    targets = [l.strip().split(',')[1] for l in open('each_page.csv').readlines()]
    KD = KumoMaster(3, targets, process2, 'process2', 10, output='detail.csv')
    KD.run()


if __name__ == '__main__':
    main()
