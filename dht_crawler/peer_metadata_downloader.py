import math
import socket
import socks
import logging
import time
from hashlib import sha1
from struct import pack, unpack
from urllib.parse import urlparse

from . import bencode
from .utils import get_rand_id

logger = logging.getLogger(__name__)

BT_HANDSHAKE_HEADER = b'BitTorrent protocol'
MESSAGE_ID = dict(
    choke=0,
    unchoke=1,
    interested=2,
    not_interested=3,
    have=4,
    bitfield=5,
    request=6,
    piece=7,
    cancel=8,
    extended=20,
)

EXTENSION_MESSAGE_ID = dict(
    handshake=0,
)


class PeerMetadataDownloader:

    def __init__(self, address, info_hash: bytes, proxy: str=None):
        self.address = address
        self.info_hash = info_hash
        self.tcp = None
        self._init_socket(proxy)

        self.ut_metadata: int = 0
        self.metadata_size: int = 0
        self.metadata: bytes = b''

        logger.debug('Downloading metadata from peer. address={}, info_hash={}, proxy={}'
                     .format(address, info_hash, proxy))

    def _init_socket(self, proxy: str=None) -> None:
        """Initialize TCP socket.

        :param proxy: Proxy address. Only support SOCKS4, SOCKS5, HTTP. 'socks5://127.0.0.1:1080'
        :return: None
        """
        parsed = urlparse(proxy)
        if parsed.scheme.upper() in socks.PROXY_TYPES and parsed.hostname and parsed.port:
            proxy_type = socks.PROXY_TYPES[parsed.scheme.upper()]
            proxy_host = parsed.hostname
            proxy_port = parsed.port
            self.tcp = socks.socksocket(family=socket.AF_INET, type=socket.SOCK_STREAM)
            self.tcp.set_proxy(proxy_type, proxy_host, proxy_port)
        else:
            self.tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            self.tcp.settimeout(4)
            ret = self.tcp.connect_ex(self.address)
            if ret != 0:
                self.tcp = None
        except ConnectionRefusedError as e:
            self.tcp = None
            logger.error('Can not establish connection with `{}`: {}'.format(self.address, str(e)))
        except TimeoutError:
            self.tcp = None
            logger.error('Connection timeout.')

    def close(self):
        if self.tcp:
            self.tcp.close()

    def send_msg(self, msg: bytes):
        length_prefix: bytes = pack('>I', len(msg))
        msg = length_prefix + msg
        self.tcp.sendall(msg)

    def send_bt_handshake(self):
        bt_header = pack('>B', len(BT_HANDSHAKE_HEADER)) + BT_HANDSHAKE_HEADER
        reserved_bytes = b'\x00\x00\x00\x00\x00\x10\x00\x00'
        peer_id: bytes = get_rand_id()
        packet = bt_header + reserved_bytes + self.info_hash + peer_id
        logger.debug('Sending BT handshake. msg={}'.format(packet))
        self.tcp.sendall(packet)

    def send_ext_handshake_msg(self):
        msg_id = pack('>B', MESSAGE_ID['extended'])
        ext_msg_id = pack('>B', EXTENSION_MESSAGE_ID['handshake'])
        payload: bytes = bencode.bencode({"m": {"ut_metadata": 2}})

        msg = msg_id + ext_msg_id + payload
        logger.debug('Sending extension handshake. msg={}'.format(msg))
        self.send_msg(msg)

    def send_request_metadata_msg(self, piece):
        msg_id = pack('>B', MESSAGE_ID['extended'])
        ext_msg_id = pack('>B', self.ut_metadata)
        payload: bytes = bencode.bencode({"msg_type": 0, "piece": piece})

        msg = msg_id + ext_msg_id + payload
        logger.debug('Sending metadata request. msg={}'.format(msg))
        self.send_msg(msg)

    def check_handshake(self, packet):
        try:
            bt_header_len, packet = ord(packet[:1]), packet[1:]
            if bt_header_len != len(BT_HANDSHAKE_HEADER):
                return False
        except TypeError:
            return False

        bt_header, packet = packet[:bt_header_len], packet[bt_header_len:]
        if bt_header != BT_HANDSHAKE_HEADER:
            return False

        info_hash = packet[8:28]
        if info_hash != self.info_hash:
            return False

        return True

    def recvall(self, length: int, timeout=5):
        data = b''
        begin = time.time()
        while len(data) < length:
            if data == b'' and time.time() - begin > timeout:
                break
            elif time.time() - begin > timeout * 4:
                break
            data += self.tcp.recv(min(length - len(data), 1024))
        return data[:length]

    def handshake(self):
        try:
            self.send_bt_handshake()
            self.send_ext_handshake_msg()

            bt_handshake_recv = self.recvall(68)
            logger.debug('Receive BT handshake message. msg={}'.format(bt_handshake_recv))
            if not self.check_handshake(bt_handshake_recv):
                return False
            logger.debug('BT handshake succeed.')

            length = self.recvall(4)
            length = unpack('>I', length)[0]
            logger.debug('length={}'.format(length))

            ext_handshake_msg = self.recvall(length)
            ext_handshake_msg = ext_handshake_msg[2:]
            logger.debug('Receive extension handshake message. msg={}'.format(ext_handshake_msg))

            ext_handshake_msg_dict: dict = bencode.bdecode(ext_handshake_msg)

            ut_metadata = ext_handshake_msg_dict.get('m', dict()).get('ut_metadata')
            metadata_size = ext_handshake_msg_dict.get('metadata_size')

            if ut_metadata is None or metadata_size is None:
                return False
            self.ut_metadata = ut_metadata
            self.metadata_size = metadata_size

            return True
        except Exception as e:
            return False

    def fetch(self):
        if self.tcp is None:
            return None
        status = self.handshake()
        if not status:
            logger.debug('Handshake failed')
            return None

        logger.debug('Handshake successful. ut_metadata={}, metadata_size={}'
                     .format(self.ut_metadata, self.metadata_size))

        while True:
            try:
                self.tcp.recv(1024)
            except socket.timeout:
                break

        metadata: bytes = b''
        for piece in range(int(math.ceil(self.metadata_size / (16.0 * 1024)))):
            self.send_request_metadata_msg(piece)
            length = self.recvall(4)
            length = unpack('>I', length)[0]
            msg = self.recvall(length)
            logger.debug('Receiving data. piece={}, length={}, actual_length={}. {}'.format(piece, length, len(msg),  msg))
            split_index = msg.index(b'ee') + 2
            header: dict = bencode.bdecode(msg[2:split_index])
            if header.get('msg_type') != 1 and header.get('piece') != piece:
                return None
            msg = msg[split_index:]
            metadata += msg

        info_hash = sha1(metadata).digest()
        logger.debug('Finish receiving data. actual_info_hash={}, actual_metadata_size={}'
                     .format(info_hash, len(metadata)))
        if info_hash == self.info_hash:
            return metadata
        else:
            return None
