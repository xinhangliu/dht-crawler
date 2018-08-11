import time
from threading import Thread
from queue import Queue

from dht_crawler import bencode
from dht_crawler.dht_crawler import DHTCrawler
from dht_crawler.peer_metadata_downloader import PeerMetadataDownloader

items = Queue()

unique_items = set()

total = 0
success_announce_peer = 0
success_get_peers = 0
announce_peer = 0
get_peers = 0
start_time = time.time()


def handler(info_hash, address, msg_type, querying_nid):
    global total, items, announce_peer, get_peers
    total += 1
    # print(msg_type, 'magnet:?xt=urn:btih:' + info_hash.hex().upper(), address)
    if info_hash in unique_items:
        return
    else:
        unique_items.add(info_hash)
    if msg_type == 'announce_peer':
        announce_peer += 1
        items.put([msg_type, info_hash, address, querying_nid])
        # print(msg_type, info_hash, address, querying_nid)
    else:
        get_peers += 1
        items.put([msg_type, info_hash, address, querying_nid])


def download():
    global success_announce_peer, items, success_get_peers
    while True:
        try:
            if items.qsize():
                item = items.get()
                d = PeerMetadataDownloader(
                    item[2],
                    item[1],
                )

                metadata = d.fetch()
                d.close()
                del d
                if metadata:
                    if item[0] == 'announce_peer':
                        success_announce_peer += 1
                    else:
                        success_get_peers += 1
                    print('SUCCEED', item[0], bencode.bdecode(metadata)['name'])
                else:
                    pass
                    # print('FAILED', item[0], item[2], item[1])
        except Exception as e:
            pass
            # print('download()', e)


def print_status():
    global total, success, items, announce_peer, get_peers
    while True:
        print('\033[32mtotal={}, announce_peer={}, get_peers={}, success_announce_peer={}, '
              'success_get_peers={}, items={}, time={}\033[0m'
              .format(total, announce_peer, get_peers, success_announce_peer, success_get_peers,
                      items.qsize(), time.time() - start_time))
        time.sleep(10)


def get_info_hash():
    dht = DHTCrawler('0.0.0.0', 9999, callback=handler)
    dht.run()


threads = [Thread(target=get_info_hash), Thread(target=print_status)]
for i in range(16):
    thread = Thread(target=download)
    threads.append(thread)

for thread in threads:
    thread.setDaemon(True)
    thread.start()

for thread in threads:
    thread.join()
