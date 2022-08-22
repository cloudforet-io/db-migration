

class MongoClient(object):

    def __init__(self, debug=False):
        self.conn = None
        self.debug = debug
        self._create_connection_pool()

    def _create_connection_pool(self):
        pass

    def execute_query(self, query):
        pass

    def execute_bulk_write(self, query):
        pass
