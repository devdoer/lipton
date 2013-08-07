#http://hy-ecomoff-job.dmop.baidu.com:8030/jobfailures.jsp?jobid=job_201205171105_640217&kind=map&cause=killed
import socket
socket.setdefaulttimeout(20)
import urllib2
import urlparse
import re
import logging


#logging.basicConfig(level=logging.INFO,format="[%(levelname)s]%(message)s")
def get_log(tracking_host, jobid):
    all_log  = ''
    #map
    url = urlparse.urljoin(tracking_host, 'jobfailures.jsp?jobid=%s&kind=map&cause=failed'%jobid)
    logging.info('fetch map nav page: %s', url)
    body = urllib2.urlopen(url).read()
    #print body
    #<a href="http://hy-hadoop-c01167.hy01.baidu.com:8060/tasklog?taskid=attempt_201205171105_640217_m_000405_1&amp;start=-4097">Last 4KB</a>    
    urls = re.findall("href=\"(http://.+?start=-4097)\"", body)
    for url in urls[:5]:
        logging.info('fetch log from: %s', url)
        try:
            body = urllib2.urlopen(url).read()
            all_log += '<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n'+body
        except:
            logging.error('fetch failed!')

    #reduce
    url = urlparse.urljoin(tracking_host, 'jobfailures.jsp?jobid=%s&kind=reduce&cause=failed'%jobid)
    logging.info('fetch reduce nav page: %s', url)
    body = urllib2.urlopen(url).read()
    #<a href="http://hy-hadoop-c01167.hy01.baidu.com:8060/tasklog?taskid=attempt_201205171105_640217_m_000405_1&amp;start=-4097">Last 4KB</a>    
    urls = re.findall("href=\"(http://.+?start=-4097)\"", body)
    for url in urls[:5]:
        logging.info('fetch log from: %s', url)
        try:
            body = urllib2.urlopen(url).read()
            all_log += '<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n'+body
        except :
            logging.error('fetch failed!')

    return all_log

    
