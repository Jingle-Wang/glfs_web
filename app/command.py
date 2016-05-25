from command_execute import *
from redis_utils import *
import json
from time import sleep
import re
from snmp import snmp_query
from config import LOCAL_HOST, DISK_PREFIX, QUERY_PERIOD
import logging

# COMMAND CONSTANT
VOLUME_CREATE = 'gluster volume create '
DISPERSE_40 = ' disperse 40 redundancy 8 '
DISPERSE_48 = ' disperse 48 redundancy 16 '
VOLUME_DELETE = 'gluster volume delete '
VOLUME_STOP = 'gluster volume stop '
VOLUME_START = 'gluster volume start '
VOLUME_QUOTA = 'gluster volume quota '
SNAPSHOT_CREATE = 'gluster snapshot create '
SNAPSHOT_DELETE = 'gluster snapshot delete '
SNAPSHOT_INFO = 'gluster snapshot info volume '
PEER_PROBE = 'gluster peer probe '
PEER_DETACH = 'gluster peer detach '
PEER_STATUS = 'gluster peer status'
VOLUME_TOP = 'gluster volume top '
WRITE_PERF = ' write-perf list-cnt 20'
READ_PERF = ' read-perf list-cnt 20'
FORCE = ' force'
ENABLE = ' enable'
LSBLK = 'lsblk'

# To do:log in file
cur_node_index = 0
BRICK_PER_NODE = 8
# <Key,Value>: <node ip,current brick index>
node_brick_map = dict()
# <Key,Value>: <node ip,total brick num>
# get this map through snmp
node_brick_num_map = dict()


def num2percent(num):
    return str(round(num * 100, 2))


def kb2gb(num):
    return str(round(num / 1048576, 1))


# Volume Commands
def volume_info():
    success, volume_list = execute_volume_info()
    if success:
        for volume in volume_list:
            Redis.hset(VOLUME_PREFIX + volume['Volume Name'], 'status', volume['Status'])


# To do:when volume stops,volume status command will show "volumeX is not started."
def volume_status():
    success, result = execute_volume_status()
    if success:
        for volume in result:
            volume_name = volume['Status of volume'].strip()
            bricks = list()
            total_used = 0
            # set volume's brick list
            for brick in volume['bricks']:
                brick_name = brick['Brick']
                l_index = brick_name.find(' ')
                address = brick_name[l_index:].strip()
                free = float(brick['Disk Space Free'][:-2])
                total = float(brick['Total Disk Space'][:-2])
                total_used += total - free
                usage = num2percent((1 - free / total))
                brick = {"address": address, "online": 'Y', "usage": usage}
                bricks.append(brick)
            Redis.set(BRICK_PREFIX + volume_name, json.dumps(bricks))

            # set volume usage
            capacity = Redis.hget(VOLUME_PREFIX + volume_name, 'capacity')
            if capacity is not None:
                volume_usage = num2percent(total_used / (float(capacity) * 1024 * 1024))
                Redis.hset(VOLUME_PREFIX + volume_name, 'usage', volume_usage)

            # add volume name to name-set
            Redis.sset(VOLUME_NAMES, volume_name)
    return result


'''
    Volume Create Strategy:
    disperse 32+8/32+16:
                  Total brick count is 40/48.
                  Choose 5/6 nodes using Round Robin.
                  Then choose 8 bricks per node.
                  Create new directory identified by volume name in each brick.
'''


def volume_create(volume_name, capacity, redundancy_ratio, transport):
    global cur_node_index
    # 32+8
    if redundancy_ratio == 25:
        total_brick = 40
        disperse = DISPERSE_40
    # 32+16
    elif redundancy_ratio == 50:
        total_brick = 48
        disperse = DISPERSE_48
    else:
        return False, 'Incorrect redundancy ratio.'
    # TB to GB
    capacity = int(capacity) * 1024
    if capacity % total_brick is not 0:
        capacity = int(capacity / total_brick + 1) * total_brick
    success, cluster_list = get_cluster_list()
    if not success:
        return False, "Can't get cluster list."
    node_num = total_brick / BRICK_PER_NODE
    bricks = ''
    for i in range(node_num):
        # Round Robin
        node_index = (i + cur_node_index) % len(cluster_list)
        bricks += generate_bricks_per_node(cluster_list[node_index], volume_name)
    cur_node_index = (node_num + cur_node_index) % len(cluster_list)
    # return VOLUME_CREATE + volume_name + disperse + bricks + FORCE
    success, result = execute_gluster(VOLUME_CREATE + volume_name + disperse + bricks + FORCE)
    return success, result


def generate_bricks_per_node(hostname, volume_name):
    node_bricks = ''
    cur_brick_index = node_brick_map[hostname]
    for i in range(BRICK_PER_NODE):
        brick_num = node_brick_num_map[hostname]
        brick_index = (i + cur_brick_index) % brick_num
        node_bricks = node_bricks + hostname + ':' + BRICK_PREFIX + str(brick_index) + '/' + volume_name + ' '

    node_brick_map[hostname] = (BRICK_PER_NODE + cur_brick_index) % node_brick_num_map[hostname]
    return node_bricks


def volume_delete(volume_name):
    success, out = execute_confirm(VOLUME_DELETE + volume_name)
    return success, out


def volume_start(volume_name):
    success, result = execute_gluster(VOLUME_START + volume_name)
    return success, result


def volume_stop(volume_name):
    cmd = VOLUME_STOP + volume_name + FORCE
    success, result = execute_confirm(cmd)
    return success, result


# Snapshot Commands

# cmd fail situation: "snapshot create: failed: Snapshot snap2 already exists"
# cmd fail situation: "snapshot create: failed: Volume (vol2) does not exist"
def snapshot_create(volume_name, snapshot_name):
    success, result = execute_gluster(SNAPSHOT_CREATE + snapshot_name + ' ' + volume_name + ' no-timestamp')
    return success, result


# cmd fail situation: "snapshot delete: failed: Snapshot (snap4) does not exist"
def snapshot_delete(snapshot_name):
    success, out = execute_confirm(SNAPSHOT_DELETE + snapshot_name)
    return success, out


# cmd fail situation: "Snapshot info : failed: Volume (volume2) does not exist"
def snapshot_info(volume_name):
    success, out = execute_gluster(SNAPSHOT_INFO + volume_name)
    if success:
        return True, generate_dic_with_subdict(out, 'snapshots', 1, 3, 5)
    else:
        return False, out


def set_snapshots():
    volume_names = list(Redis.sget(VOLUME_NAMES))
    for volume_name in volume_names:
        success, out = snapshot_info(volume_name)
        if success:
            Redis.set(SNAPSHOT_PREFIX + volume_name, json.dumps(out['snapshots']))


# Peer Commands
def peer_probe(hostname):
    success, result = execute_gluster(PEER_PROBE + hostname)
    return success, result


def peer_detach(hostname):
    success, result = execute_gluster(PEER_DETACH + hostname + FORCE)
    return success, result


def peer_status():
    success, out = execute_gluster(PEER_STATUS)
    if success:
        return True, generate_dic_with_subdict(out, 'peers', 0, 1, 4)
    else:
        return False, out


def pool_list():
    success, result = execute_pool_list()
    if success:
        Redis.set(CLUSTER_LIST, json.dumps(result))
    return result


def query_cluster_disks():
    machines = pool_list()
    disk_dict = dict()
    for machine in machines:
        disk_dict[machine['hostname']] = get_machine_disks(machine['hostname'])
    Redis.set(CLUSTER_DISKS, json.dumps(disk_dict))


def volume_quota_enable(volume_name):
    return execute_gluster(VOLUME_QUOTA + volume_name + ENABLE)


def volume_perf(volume_name, read=True):
    if read:
        success, out = execute_gluster(VOLUME_TOP + volume_name + READ_PERF)
    else:
        success, out = execute_gluster(VOLUME_TOP + volume_name + WRITE_PERF)
    perf = 0
    if success:

        out = out.split('\n')
        for line in out:
            line = line.strip().split(' ')
            if len(line) > 0 and line[0].isdigit():
                perf += int(line[0])
    return success, perf


def get_machine_disks(hostname):
    if hostname == LOCAL_HOST:
        success, disk_info = execute_gluster(LSBLK)
    else:
        success, disk_info = execute_ssh(hostname, LSBLK)
    disks = list()
    if not success:
        return disks
    else:
        disk_info = disk_info.split("\n")
        for line_data in disk_info:
            if re.search('disk', line_data, re.IGNORECASE):
                line_data_token = re.split(' +', line_data)
                if line_data_token[-1]:
                    disks.append(line_data_token[-1])
        return disks


def get_cluster_list():
    cluster_list = json.loads(Redis.get(CLUSTER_LIST))
    machines = list()
    if len(cluster_list) == 0:
        logging.warning("query_machine_resource: Can't get cluster list.")
        return False, machines
    else:
        for machine in cluster_list:
            machines.append(machine['hostname'])
        return True, machines


def monitor_resource(hostname):
    storage_size = snmp_query('hrStorageSize', hostname, 'public')
    storage_used = snmp_query('hrStorageUsed', hostname, 'public')
    storage_descr = snmp_query('hrStorageDescr', hostname, 'public')
    # To do:raise err if snmp can't get machine info.

    # physical memory
    len_storage_descr = len(storage_descr)
    if len_storage_descr < 1:
        return
    memory = dict()
    memory_usage = 0
    if storage_descr[0] == 'Physical memory':
        memory['size'] = kb2gb(float(storage_size[0]))
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
        if DISK_PREFIX in storage_descr[i]:
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
        cpu_usages.append(processor_load[i])
        cpus.append(cpu)
        i += 1

    resource = dict()
    resource['hostname'] = hostname
    resource['memory'] = memory
    resource['disks'] = disks
    resource['cpus'] = cpus
    return resource, memory_usage, cpu_usages


def query_machine_resource():
    success, cluster_list = get_cluster_list()
    if not success:
        return
    resource_dict = dict()
    # To do: handle error when any machine's snmp service shut down
    for machine in cluster_list:
        resource_dict[machine], memory_usage, cpu_usages = monitor_resource(machine)
        Redis.lpush(MEMORY_USAGE_PREFIX + machine, memory_usage)
        i = 0
        while i < len(resource_dict[machine]['cpus']):
            Redis.lpush(CPU_USAGE_PREFIX + machine + ':' + str(i), cpu_usages[i])
            i += 1
    Redis.set(CLUSTER_RESOURCE, json.dumps(resource_dict))


def query_machine_resource_local():
    resource_dict = dict()
    resource_dict[LOCAL_HOST], memory_usage, cpu_usages = monitor_resource(LOCAL_HOST)
    Redis.lpush(MEMORY_USAGE_PREFIX + LOCAL_HOST, memory_usage)
    i = 0
    while i < len(resource_dict[LOCAL_HOST]['cpus']):
        Redis.lpush(CPU_USAGE_PREFIX + LOCAL_HOST + ':' + str(i), cpu_usages[i])
        i += 1
    Redis.set(CLUSTER_RESOURCE, json.dumps(resource_dict))


def query_volume_perf():
    volume_names = list(Redis.sget(VOLUME_NAMES))
    for volume_name in volume_names:
        success, read_perf = volume_perf(volume_name)
        if success:
            Redis.lpush(READ_SPEED_PREFIX + volume_name, read_perf)
        success, write_perf = volume_perf(volume_name, False)
        if success:
            Redis.lpush(WRITE_SPEED_PREFIX + volume_name, write_perf)


# thread function
def query_periodically():
    while True:
        volume_status()
        volume_info()
        pool_list()
        query_cluster_disks()
        query_machine_resource()
        query_volume_perf()
        sleep(QUERY_PERIOD)


'''
# unused Brick Commands
def volume_addbrick(volume_name, new_brick):
    # cmd = [GLUSTER, VOLUME, 'add-brick', volume_name, new_brick]
    success, result = execute_gluster('gluster volume add-brick '+volume_name +' '+new_brick+' force')
    return success, result


def volume_removebrick(volume_name, brick, force=True):
    cmd = [GLUSTER, VOLUME, 'remove-brick', volume_name, brick]
    if force:
        cmd += ['force']
    execute_confirm(cmd)


def volume_replacebrick(volume_name, brick, new_brick):
    # cmd = [GLUSTER, VOLUME, 'replace-brick', volume_name, brick, new_brick, 'commit', 'force']
    success, result = execute_gluster('gluster volume replace-brick '+volume_name +' '+brick+' '+new_brick+' commit force')
    return success, result
'''
