import redis
import docker


class SharedMemoryManager():
    def __init__(self, host='localhost', port=6379, db=0):
        client = docker.from_env()
        client.images.pull('redis')
        container = client.containers.run('redis', detach=True, name='my-redis', ports={f'{port}/tcp': port})
        self.container_id = container.id
        self.db = redis.Redis(host=host, port=port, db=db)

    def set_value(self, key, value):
        self.db.set(key, value)

    def get_value(self, key):
        return self.db.get(key).decode()

    def __del__(self):
        client = docker.from_env()
        client.containers.get(self.container_id).stop()
