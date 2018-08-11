import logging
import socket
import socks
from time import sleep
from collections import deque
from urllib.parse import urlparse
from threading import Thread

from . import bencode
from .utils import get_rand_id, get_nearby_id, decode_compact_nodes_info
from .constants import TRANSACTION_ID_LENGTH, BOOTSTRAP_NODES, MAX_NODE_QSIZE, TOKEN_LENGTH, DEFAULT_SENDING_INTERVAL

logger = logging.getLogger(__name__)


class DHTCrawler:
    """DHT crawler.

    Operating procedures:
        1. Join the DHT network with public nodes;
        2. Expand the routing table. Keep sending `find_node` request to the known nodes, get new nodes;
        3. Wait for the nodes to send requests/responses to you. Extract info_hash from the messages.
    """

    def __init__(self,
                 bind_host: str,
                 bind_port: int,
                 callback=None,
                 sending_interval: float=DEFAULT_SENDING_INTERVAL,
                 proxy: str=None):
        """Init.

        :param bind_host: UDP ip/hostname.
        :param bind_port: UDP port.
        :param callback: Callback function for handling crawled info_hash.
        :param sending_interval: Interval of sending messages.
        :param proxy: Proxy for UDP connection.
        """
        self.bind_host = bind_host
        self.bind_port = bind_port
        self.callback = callback
        self.sending_interval = sending_interval
        self.nid: bytes = get_rand_id()
        self.nodes = deque(maxlen=MAX_NODE_QSIZE)
        self._init_socket(proxy)

    def _init_socket(self, proxy: str=None) -> None:
        """Initialize UDP socket.

        :param proxy: Proxy address. Only support SOCKS4, SOCKS5, HTTP. 'socks5://127.0.0.1:1080'
        :return: None
        """
        parsed = urlparse(proxy)
        if parsed.scheme.upper() in socks.PROXY_TYPES and parsed.hostname and parsed.port:
            proxy_type = socks.PROXY_TYPES[parsed.scheme.upper()]
            proxy_host = parsed.hostname
            proxy_port = parsed.port
            self.udp = socks.socksocket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
            self.udp.set_proxy(proxy_type, proxy_host, proxy_port)
        else:
            self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.udp.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.udp.bind((self.bind_host, self.bind_port))

    def run(self):
        """Starting crawler."""

        threads = [
            Thread(target=self.keep_sending),
            Thread(target=self.keep_receiving),
        ]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

    def _send_krpc(self, msg: dict, address: tuple) -> None:
        """Send messages under KRPC Protocol.
        For more information, please visit <http://www.bittorrent.org/beps/bep_0005.html>.

        :param msg: The messages to send.
        :param address: Target address. (host, port)
        :return: None
        """
        try:
            self.udp.sendto(bencode.bencode(msg), address)
        # except PermissionError:
            # logger.warning('The node address is a broadcast address, sending message to it is not allowed. address={}'
            #                .format(address))
        except Exception:
            pass

    def send_find_node(self, address, source_nid: bytes=None) -> None:
        """Send find_node request.
        Find node is used to find the contact information for a node given its ID. "q" == "find_node"
        A find_node query has two arguments, "id" containing the node ID of the querying node,
        and "target" containing the ID of the node sought by the queryer.

        :param address: Address of the target server. (host, port)
        :param source_nid: Source node ID.
        :return: None
        """
        nid = get_nearby_id(source_nid) if source_nid else self.nid
        tid = get_rand_id(TRANSACTION_ID_LENGTH)
        msg = dict(
            t=tid,
            y='q',
            q='find_node',
            a=dict(id=nid, target=get_rand_id()),
        )
        self._send_krpc(msg, address)

    def bootstrap(self):
        """Join the DHT network with public nodes."""

        for address in BOOTSTRAP_NODES:
            self.send_find_node(address)

    def keep_sending(self) -> None:
        """Keep sending `find_node` request.

        :return: None
        """
        while True:
            try:
                node = self.nodes.popleft()
                self.send_find_node((node.host, node.port), node.nid)
                sleep(self.sending_interval)
            except IndexError:
                # Join DHT if nodes queue is empty
                self.bootstrap()
            except Exception:
                pass

    def keep_receiving(self) -> None:
        """Keep receiving messages.

        :return: None
        """
        self.bootstrap()
        while True:
            try:
                (datagram, address) = self.udp.recvfrom(65536)
                msg: dict = bencode.bdecode(datagram)
                self._on_message(msg, address)
            except Exception:
                pass

    def _on_message(self, msg: dict, address: tuple):
        """Handle received message.

        :param msg: Received message.
        :param address: The address which the message sending from. (host, port)
        :return:
        """
        try:
            if msg['y'] == 'r':
                if msg['r'].get('nodes'):
                    self._on_find_node_response(msg)
            elif msg['y'] == 'q':
                if msg['q'] == 'get_peers':
                    self._on_get_peers_request(msg, address)
                elif msg['q'] == 'announce_peer':
                    self._on_announce_peer_request(msg, address)
                # elif msg['q'] == 'ping':
                #     self._on_ping_request(msg, address)
        except Exception:
            pass

    def _on_find_node_response(self, msg: dict) -> None:
        """Process `find_node` response.
        When a node receives a `find_node` query, it should respond with a key "nodes" and value of a string
        containing the compact node info for the target node or the K (8) closest good nodes in its own routing table.

        :param msg: Response message of other DHT node.
        :return: None
        """
        nodes = decode_compact_nodes_info(msg["r"]["nodes"])
        self.nodes.extend(nodes)

    def _on_get_peers_request(self, msg: dict, address: tuple) -> None:
        """Handle `get_peers` request.
        One of `get_peers` request's arguments is info_hash, which represent a resource (magnet link).
        Sending a response, to avoid the other node think the crawler as a "bad" node.

        :param msg: Request message of the other DHT node.
        :param address: Address of the DHT node.
        :return: None
        """
        try:
            info_hash = msg['a']['info_hash']
            querying_nid = msg['a']['id']
            msg_type = 'get_peers'
            self._handle_info_hash(info_hash, address, msg_type, querying_nid)
        except KeyError:
            pass

        try:
            resp_msg = dict(
                t=msg['t'],
                y='r',
                r=dict(
                    id=self.nid,
                    nodes="",
                    token=msg['a']['info_hash'][:TOKEN_LENGTH],
                )
            )
            self._send_krpc(resp_msg, address)
        except Exception:
            pass

    def _on_announce_peer_request(self, msg: dict, address: tuple) -> None:
        """Handle `announce_peer` request.

        :param msg: Request message of the other DHT node.
        :param address: Address of the DHT node.
        :return: None
        """
        try:
            info_hash = msg["a"]["info_hash"]
            querying_nid = msg['a']['id']
            # token = msg["a"]["token"]
            msg_type = 'announce_peer'

            address = list(address)

            # if info_hash[:TOKEN_LENGTH] == token:
            if msg['a'].get('implied_port') == 0:
                address[1] = msg['a']['port']
            self._handle_info_hash(info_hash, tuple(address), msg_type, querying_nid)
        except Exception:
            pass

    def _on_ping_request(self, msg: dict, address: tuple) -> None:
        """Handle `ping` request.
        The most basic query is a `ping`. "q" = "ping" A ping query has a single argument,
        "id" the value is a 20-byte string containing the senders node ID in network byte order.
        The appropriate response to a ping has a single key "id" containing the node ID of the responding node.

        :param msg: Request message of the other DHT node.
        :param address: Address of the DHT node.
        :return: None
        """
        try:
            resp_msg = dict(
                t=msg['t'],
                y='r',
                r=dict(id=self.nid),
            )
            self._send_krpc(resp_msg, address)
        except Exception:
            pass

    def _handle_info_hash(self, info_hash: bytes, address: tuple, msg_type: str, querying_nid):
        """Default info_hash handler.

        :param info_hash:
        :param address:
        :param msg_type:
        :param querying_nid:
        :return:
        """
        if self.callback:
            self.callback(info_hash, address, msg_type, querying_nid)
        else:
            logger.info('{}: {} - {}'.format(msg_type, info_hash.hex().upper(), address))
