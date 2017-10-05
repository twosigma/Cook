import logging

logging.getLogger('').handlers = []
logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.DEBUG)
