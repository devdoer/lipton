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
    urls = re.findall("href=\"(http://.+?start=-4097)\"", body)
    for url in urls[:5]:
        logging.info('fetch log from: %s', url)
        try:
            body = urllib2.urlopen(url).read()
            all_log += '<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n'+body
        except :
            logging.error('fetch failed!')

    return all_log

    
