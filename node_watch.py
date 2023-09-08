#! /bin/python3

# Configuration update watcher

from kazoo.client import KazooClient, KazooState   

import logging
import time
import json 
import random
import signal

def sigint_handler(sig, frame):
    logging.info('Exiting')
    exit(0)

signal.signal(signal.SIGINT, sigint_handler)
logging.basicConfig()
logging.root.setLevel(logging.INFO)

HOST = 'localhost:2181'
PATH = 'config/rootconfig'

zk = KazooClient(hosts=HOST)
zk.start()

if zk.state != KazooState.CONNECTED:
    logging.info("Can't connect to Zookeeper: " + str(zk.state))
    exit(0)

# If the path doesn't exist, it creates it.
zk.ensure_path(PATH)

def get_dynamic_data():
    data = {'A': random.randint(0, 10),
            'B:': random.randint(11, 20)
            }
    return data

# Add a watch for the path. This will be called whenever there 
# is a change in the node.
@zk.DataWatch(PATH)
def watch(data, stat):
    logging.info('State changed: ' + data.decode('utf-8'))
    logging.info('Version: ' + str(stat.version))

def generate_data():
    # Press CTRL-C to abort
    while True:
        data = get_dynamic_data()
        zk.set(PATH, json.dumps(data).encode('utf-8'))
        time.sleep(5)

if __name__ == '__main__':
    generate_data()

