# Length of node ID
NODE_ID_LENGTH: int = 20

# Length of transaction ID
TRANSACTION_ID_LENGTH = 2

# Length of token
TOKEN_LENGTH = 2

# Max node queue size
MAX_NODE_QSIZE: int = 1000

DEFAULT_SENDING_INTERVAL = 0.00001

# Public nodes of DHT network
BOOTSTRAP_NODES = (
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881),
)
