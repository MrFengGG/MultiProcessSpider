import time
import urllib.parse
import threading
import multiprocessing
import re
import lxml
import datetime

from MongoCache import MongoCache
from MongoQueue import MongoQueue
from Downloader import Downloader
from bs4 import BeautifulSoup
from download_source_callback import download_source_callback

#线程
SLEEP_TIME = 1


def normalize(seed_url, link):
    '''

    用于将绝对路径转换为相对路径
    '''
    link, no_need = urllib.parse.urldefrag(link)

    return urllib.parse.urljoin(seed_url, link)


def same_domain(url1, url2):
    '''
    判断域名书否相同
    '''
    return urllib.parse.urlparse(url1).netloc == urllib.parse.urlparse(url2).netloc


def get_links(html):
    '''
    获得一个页面上的所有链接
    '''
    bs = BeautifulSoup(html, "lxml")
    link_labels = bs.find_all("a")
    # for link in link_labels:
    return [link_label.get('href', "default") for link_label in link_labels]

def threaded_crawler(seed_url,delay=5,link_regiex = ".*",download_source_callback=None,cache=None,resource_regiex=None,user_agent="wswp",proxies=None,num_retries=1,max_threads=10,timeout=60,max_urls=-1):
    '''
    多线程爬虫
    :param seed_url: 种子url
    :param delay: 延时
    :param cache: 缓存方式
    :param link_regiex: 链接需要符合的正则表达式
    :param resource_regiex: 资源页需要符合的正则表达式
    :param download_source_callback: 下载资源的类
    :param user_agent: 主机名
    :param proxies: 代理IP列表
    :param num_retries: 重下载次数
    :param max_threads: 最大线程数量
    :param timeout: 默认超时时间
    :param max_urls: 下载的最大链接数量
    :return: 
    '''
    #构造一个用于储存未下载网页的队列

    crawl_queue = MongoQueue()
    #清空队列
    crawl_queue.clear()
    #向队列中加入种子url
    crawl_queue.push(seed_url)
    #构造html下载器
    D = Downloader(cache=cache,delay=delay,user_agent=user_agent,proxies=proxies,num_retries=num_retries,timeout=timeout)

    def process_queue():
        while True:
            if crawl_queue.len_of_downloaded()[0] == 0 or crawl_queue.len_of_downloaded()[1] > max_urls:
                #当可下载的url数量为0或者已经下载的url数量达到最大时,线程退出
                print("线程退出")
                return
            #用于存储从页面上下载的链接
            links = []
            try:
                #从队列中获取一个连接
                url = crawl_queue.pop()
            except KeyError:
                break
            else:
                html = D(url)
                #如果有资源下载器,并且是资源页面,下载该页面上的资源
                if resource_regiex and re.match(resource_regiex,url):
                    download_source(url,html)
                links.extend([link for link in get_links(html) if re.match(link_regiex, link)])
                for link in links:
                    link = normalize(seed_url,link)
                    #将从页面下载的链接加入到队列中
                    crawl_queue.push(link)
                #将已经下载的url状态置为已下载
                crawl_queue.complete(url)

    threads = []
    #当当前存在线程或者队列中有可下载的链接时,启动线程
    while threads or crawl_queue.len_of_downloaded()[0]>0:
        if crawl_queue.len_of_downloaded()[1] > max_urls:
            #达到最大下载数量,退出
            break
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)
        while len(threads) < max_threads and crawl_queue.len_of_downloaded()[0]!=0:
            thread = threading.Thread(target=process_queue)
            thread.setDaemon(True)
            thread.start()
            print("开启线程数量为", len(threads))
            threads.append(thread)
        time.sleep(SLEEP_TIME)

def process_crawler(args,**kwargs):
    '''
    多进程爬虫
    :param args: 传入的参数
    :param kwargs: 传入的键值参数
    '''
    #获得cpu数量
    num_cpu = multiprocessing.cpu_count()
    print("开启"+str(num_cpu)+"个进程")
    process=[]
    for i in range(1):
        cache=MongoCache()
        p = multiprocessing.Process(target=threaded_crawler,args=args,kwargs={cache:cache})
        p.start()
        process.append(p)
    for p in process:
        p.join()

if __name__ == "__main__":
    start = datetime.datetime.utcnow()
    process_crawler("http://www.baidu.com",max_urls=100,cache=MongoCache(),user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36")
    end = datetime.datetime.utcnow()
    print("网站网页已全部下载完毕,用时",end-start,"秒")
