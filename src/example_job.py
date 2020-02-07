from master import KumoMaster

def main():
    targets = ["https://sz.lianjia.com/ershoufang/pg{}/".format(i) for i in range(1,50)]
    process = lambda soup: ','.join([i.find('a')['href'] for i in soup.find_all('div', class_ = "info clear")])
    KD = KumoMaster(3, targets, process)
    KD.run()

if __name__ == '__main__':
    main()