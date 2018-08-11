import sys
import logging


log_format = '%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d - %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=log_format)
