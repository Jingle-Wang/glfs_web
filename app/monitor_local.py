from snmp import snmp_query
from config import LOCAL_HOST, DISK_NAME,QUERY_PERIOD
from redis_utils import *
import json
from time import sleep


def num2percent(num):
    return str(round(num * 100, 2))


def kb2gb(num):
    return str(round(num / 1048576, 1))


def query_machine_resource_local():
    resources = list()
    resource, memory_usage, cpu_usages = monitor_resource(LOCAL_HOST)
    Redis.lpush(MEMORY_USAGE_PREFIX + LOCAL_HOST, memory_usage)
    i = 0
    while i < len(resource['cpus']):
        Redis.lpush(CPU_USAGE_PREFIX + LOCAL_HOST + ':' + str(i), cpu_usages[i])
        i += 1
    resources.append(resource)
    Redis.set(CLUSTER_RESOURCE, json.dumps(resources))


def monitor_resource(hostname):
    storage_size = snmp_query('hrStorageSize', hostname, 'public')
    storage_used = snmp_query('hrStorageUsed', hostname, 'public')
    storage_descr = snmp_query('hrStorageDescr', hostname, 'public')
    # To do:raise err if snmp can't get machine info.

    # physical & virtual memory
    len_storage_descr = len(storage_descr)
    if len_storage_descr < 1:
        return
    memory = dict()
    # virtual_mem = dict()
    memory_usage = 0
    if storage_descr[0] == 'Physical memory':
        memory['size'] = kb2gb(float(storage_size[0]))
        # memory['usage'] = num2percent(float(storage_used[0]) / float(storage_size[0]))
        memory_usage = num2percent(float(storage_used[0]) / float(storage_size[0]))
    '''
    if storage_descr[1] == 'Virtual memory':
        virtual_mem['size'] = kb2gb(int(storage_size[1]))
        virtual_mem['usage'] = num2percent(float(storage_used[1]) / float(storage_size[1]))
    '''

    # disks
    i = 5
    disks = list()
    while i < len_storage_descr:
        # /brick/brickX is appointed disk mount directory
        if DISK_NAME in storage_descr[i]:
            disk = dict()
            disk['name'] = storage_descr[i]
            disk['size'] = kb2gb(int(storage_size[i]))
            disk['usage'] = num2percent(float(storage_used[i]) / float(storage_size[i]))
            disks.append(disk)
        i += 1

    # cpus
    processor_load = snmp_query('hrProcessorLoad', hostname, 'public')
    device_descr = snmp_query('hrDeviceDescr', hostname, 'public')
    cpus = list()
    cpu_usages = list()
    i = 0
    while i < len(processor_load):
        cpu = dict()
        cpu['name'] = device_descr[i]
        # cpu['usage'] = processor_load[i]
        cpu_usages.append(processor_load[i])
        cpus.append(cpu)
        i += 1

    resource = dict()
    resource['hostname'] = hostname
    resource['memory'] = memory
    # resource['virtualmem'] = virtual_mem
    resource['disks'] = disks
    resource['cpus'] = cpus
    return resource, memory_usage, cpu_usages


# thread function
def query_periodically():
    while True:
        query_machine_resource_local()
        sleep(QUERY_PERIOD)
