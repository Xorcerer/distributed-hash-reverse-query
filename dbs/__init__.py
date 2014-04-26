import kyotocabinet as kc

import leveldb


__all__ = ['LevelDB', 'KyotoCabinetDB']


class LevelDB(object):

    def __init__(self, db_name):
        self.db = leveldb.DB(db_name, default_fill_cache=False,
                             create_if_missing=True)

    def put(self, key, value):
        return self.db.put(key, value)

    def get(self, key):
        return self.db.get(key)

    def close(self):
        return self.db.close()


class KyotoCabinetDB(object):

    def __init__(self, db_name):
        self.db = kc.DB()
        assert self.db.open(db_name, kc.DB.OWRITER | kc.DB.OCREATE),\
            'Failed to open db "%s": "%s".' % (db_name, self.db.error())

    def put(self, key, value):
        return self.db.set(key, value)

    def get(self, key):
        return self.db.get(key)

    def close(self):
        return self.db.close()
