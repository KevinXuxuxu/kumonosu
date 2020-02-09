from master import KumoMaster

def main():
    targets = ["https://sz.lianjia.com/ershoufang/pg{}/".format(i) for i in range(1,11)]
    process = lambda soup: [i.find('a')['href'] for i in soup.find_all('div', class_ = "info clear")]
    KD = KumoMaster(3, targets, process, 3, flat=True)
    KD.run()

if __name__ == '__main__':
    main()
