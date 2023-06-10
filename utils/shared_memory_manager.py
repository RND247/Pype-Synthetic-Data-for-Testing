import redis
import docker


class SharedMemoryManager():
    def __init__(self, host='localhost', port=6379, db=0):
        self.container_id = None
        self.port = port
        self.db = redis.Redis(host=host, port=port, db=db)

    def create_shared_memory(self):
        client = docker.from_env()
        client.images.pull('redis')
        container = client.containers.run('redis', detach=True, name='my-redis', ports={f'{self.port}/tcp': self.port})
        self.container_id = container.id
        
    def set_value(self, key, value):
        pipeline = self.db.pipeline()

        # Watch the key for changes
        pipeline.watch(key)

        # Check if the key already exists
        if pipeline.exists(key):
            return self.get_value(key)
        else:
            # Start the transaction
            pipeline.multi()

            # Set the value for the key
            pipeline.set(key, value)

            # Execute the transaction
            pipeline.execute()

            return value

    def get_value(self, key):
        val = self.db.get(key)
        if val:
            return val.decode()

    def stop_shared_memory(self):
        if self.container_id:
            client = docker.from_env()
            client.containers.get(self.container_id).stop()
