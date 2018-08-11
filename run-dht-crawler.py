from dht_crawler.dht_crawler import DHTCrawler
import time

total = 0


def handler(info_hash, address, msg_type, querying_nid):
    global total
    total += 1
    print('magnet:?xt=urn:btih:{}'.format(info_hash.hex().upper()))


dht = DHTCrawler('0.0.0.0', 9999, callback=handler, proxy=None)

start = time.time()
try:
    dht.run()
except KeyboardInterrupt:
    spend_time = time.time() - start
    print('time={}, total={}, speed={} / sec'.format(spend_time, total, total / spend_time))
