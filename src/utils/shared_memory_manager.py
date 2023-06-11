import redis


class SharedMemoryManager():
    def __init__(self, host='localhost', port=6379, db=0):
        self.db = redis.Redis(host=host, port=port, db=db)

    def set_value(self, key, value):
        pipeline = self.db.pipeline()
        # Watch the key for changes
        pipeline.watch(key)
        # Check if the key already exists
        if pipeline.exists(key):
            return self.get_value(key)
        pipeline.multi()
        pipeline.set(key, value)
        pipeline.execute()
        return value

    def get_value(self, key):
        val = self.db.get(key)
        if val:
            return val.decode()
