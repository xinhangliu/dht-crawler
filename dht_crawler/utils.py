import os
from struct import unpack
import socket
from io import BytesIO

from .constants import NODE_ID_LENGTH


class KNode:
    """DHT nodes class."""

    def __init__(self, nid: str, host: str, port: int):
        """
        :param nid: Node ID
        :param host: Host or ip address of the node
        :param port: Port of the node
        """
        self.nid = nid
        self.host = host
        self.port = port


def get_rand_id(length: int=None) -> bytes:
    """Generate random node ID.

    :param length: Length of ID.
    """
    if length is None:
        length = NODE_ID_LENGTH
    return os.urandom(length)


def get_nearby_id(target: bytes, end: int=int(0.7 * NODE_ID_LENGTH)) -> bytes:
    """Generate nearby Node ID of the target node ID.
    In Kademlia, the distance metric is XOR and the result is interpreted as an unsigned integer.
    distance(A, B) = |A xor B| Smaller values are closer.

    :param target: Target node ID
    :param end: Length pf the static prefix of target node ID.
    """
    return target[:end] + get_rand_id(NODE_ID_LENGTH - end)


def decode_compact_nodes_info(compact_nodes_info: bytes) -> list:
    """Decode Compact node info
    Contact information for nodes is encoded as a 26-byte string. Also known as "Compact node info".
    The 20-byte Node ID in network byte order has the compact IP-address/port info concatenated to the end.

    Contact information for peers is encoded as a 6-byte string. Also known as "Compact IP-address/port info".
    The 4-byte IP address is in network byte order with the 2 byte port in network byte order concatenated onto the end.

    :param compact_nodes_info: Concatenated bytes of several "Compact node info".
    :return: A list of <class KNode> objects.
    """
    nodes_list = []
    length = NODE_ID_LENGTH + 6
    if len(compact_nodes_info) % length != 0:
        return nodes_list

    compact_nodes_info = BytesIO(compact_nodes_info)

    while True:
        nid = compact_nodes_info.read(NODE_ID_LENGTH)
        if not nid:
            break
        host = socket.inet_ntoa(compact_nodes_info.read(4))
        port = unpack("!H", compact_nodes_info.read(2))[0]

        node = KNode(nid, host, port)
        nodes_list.append(node)

    return nodes_list
