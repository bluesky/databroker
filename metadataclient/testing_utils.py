from metadataclient.api import server_connect

testing_config = {'host': 'localhost', 'port': 7770,
                  'mongo_server': 'localhost', 'mongo_port': 27017,
                  'mongo_database': 'test'}

def start_mongo():
    pass


def start_mdservice():
    pass


def mds_setup():
    server_connect(host=testing_config['host'],
                   port=testing_config['port'])