import redis

# Redis Key

# hash object
CAPACITY = 'capacity'
VOLUME_PREFIX = 'volume:'
SNAPSHOT_PREFIX = 'snapshot:'
BRICK_PREFIX = 'brick:'
# set object
VOLUME_NAMES = 'volume:names'
# single object
CLUSTER_DISKS = 'cluster:disks'
CLUSTER_LIST = 'cluster:list'
CLUSTER_RESOURCE = 'cluster:resource'

# list object

# memory_usage:192.168.1.150
MEMORY_USAGE_PREFIX = 'memory_usage:'
# cpu_usage:192.168.1.150:1 cpu_usage:192.168.1.150:2 etc
CPU_USAGE_PREFIX = 'cpu_usage:'
READ_SPEED_PREFIX = 'read_speed:'
WRITE_SPEED_PREFIX = 'write_speed:'


# This class is wrapper for a redis instance
class Redis:
    r = redis.StrictRedis(host='localhost', port=6379, db=0)

    @staticmethod
    def set(name, value):
        Redis.r.set(name, value)

    @staticmethod
    def get(name):
        return Redis.r.get(name)

    @staticmethod
    def delete(name):
        Redis.r.delete(name)

    @staticmethod
    def hset(name, key, value):
        Redis.r.hset(name, key, value)

    @staticmethod
    def hmset(name, mapping):
        Redis.r.hmset(name, mapping)

    @staticmethod
    def hget(name, key):
        return Redis.r.hget(name, key)

    @staticmethod
    def hgetall(name):
        return Redis.r.hgetall(name)

    @staticmethod
    def sset(name, value):
        Redis.r.sadd(name, value)

    @staticmethod
    def sget(name):
        return Redis.r.smembers(name)

    @staticmethod
    def srem(name, key):
        return Redis.r.srem(name, key)

    # append to list
    @staticmethod
    def lpush(name, key):
        Redis.r.rpush(name, key)

    @staticmethod
    def lrange(name, start, end):
        return Redis.r.lrange(name, start, end)
