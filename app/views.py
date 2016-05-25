from flask import Flask, request, jsonify
from redis_utils import *
from command import snapshot_create, snapshot_delete, volume_delete, volume_start, volume_stop, volume_create
import json
from config import MONITOR_LIST_LEN, PERF_LIST_LEN

app = Flask(__name__)


# page 0: system overview
@app.route('/overview/capacity')
def get_overview_capacity():
    try:
        return jsonify(Redis.hgetall(CAPACITY))
    except TypeError:
        return jsonify(success=False, message='No Capacity information at present.')


@app.route('/overview/volumes')
def get_overview_volumes():
    try:
        volume_names = list(Redis.sget(VOLUME_NAMES))
        volumes = list()
        for volume_name in volume_names:
            volumes.append(Redis.hgetall(VOLUME_PREFIX + volume_name))
        return jsonify(volume_nums=len(volume_names), volumes=volumes)
    except TypeError:
        return jsonify(success=False, message='No volume information at present.')


# page 1: volume management
@app.route('/volume/names')
def get_volume_names():
    try:
        volume_names = list(Redis.sget(VOLUME_NAMES))
        return jsonify(names=volume_names)
    except TypeError:
        return jsonify(success=False, message='No Volume names information at present.')


@app.route('/volume/<string:volume_name>')
def get_volume_info(volume_name):
    usage = Redis.hget(VOLUME_PREFIX + volume_name, 'usage')
    bricks = Redis.get(BRICK_PREFIX + volume_name)
    snapshots = Redis.get(SNAPSHOT_PREFIX + volume_name)
    return jsonify(usage=usage, bricks=bricks, snapshots=snapshots)


@app.route('/volume/remove/<string:volume_name>')
def delete_volume(volume_name):
    success, out = volume_delete(volume_name)
    # remove volume_name in volume:names set
    if success:
        Redis.srem(VOLUME_NAMES, volume_name)
    return jsonify(success=success, message=out)


@app.route('/volume/<string:volume_name>/speed')
def volume_perf(volume_name):
    try:
        read = Redis.lrange(READ_SPEED_PREFIX + volume_name, -PERF_LIST_LEN, -1)
        write = Redis.lrange(WRITE_SPEED_PREFIX + volume_name, -PERF_LIST_LEN, -1)
        fill_list(read)
        fill_list(write)
        return jsonify(success=True, read_speed=read, write_speed=write)
    except KeyError:
        return jsonify(success=False, message='No Volume performance information at present.')


@app.route('/volume/<string:volume_name>/self/start')
def start_volume(volume_name):
    success, out = volume_start(volume_name)
    return jsonify(success=success, message=out)


@app.route('/volume/<string:volume_name>/self/stop')
def stop_volume(volume_name):
    success, out = volume_stop(volume_name)
    return jsonify(success=success, message=out)


# To do
@app.route('/volume/add')
def add_volume():
    try:
        volume_name = request.form['name']
        capacity = request.form['capacity']
        transport = request.form['transport']
        redundancy_ratio = request.form['redundancy_ratio']
        success, out = volume_create(volume_name, capacity, redundancy_ratio, transport)
        return jsonify(success=success, message=out)
    except KeyError, e:
        return jsonify(success=False, message='Request Form needs Key ' + str(e) + '.')


@app.route('/volume/<string:volume_name>/snapshot/add/<string:snapshot_name>')
def add_snapshot(volume_name, snapshot_name):
    success, out = snapshot_create(volume_name, snapshot_name)
    return jsonify(success=success, message=out)


@app.route('/snapshot/remove/<string:snapshot_name>')
def delete_snapshot(snapshot_name):
    success, out = snapshot_delete(snapshot_name)
    return jsonify(success=success, message=out)


# page 2: system management
@app.route('/cluster/info')
def get_cluster_info():
    cluster_list = json.loads(Redis.get(CLUSTER_LIST))
    cluster_disks = json.loads(Redis.get(CLUSTER_DISKS))
    try:
        for machine in cluster_list:
            machine['bricks'] = cluster_disks[machine['hostname']]
        return jsonify(cluster=cluster_list)
    except TypeError:
        return jsonify(success=False, message='No Cluster information at present.')


# page 3: performance monitor
@app.route('/monitor/info')
def get_monitor_info():
    try:
        cluster_resource = Redis.get(CLUSTER_RESOURCE)
        if cluster_resource == 'null':
            return jsonify(success=False)
        cluster_resource = json.loads(cluster_resource)
        i = 0
        while i < len(cluster_resource):
            machine = cluster_resource[i]
            hostname = machine['hostname']
            memory_usage = Redis.lrange(MEMORY_USAGE_PREFIX + hostname, -MONITOR_LIST_LEN, -1)
            fill_list(memory_usage)
            cluster_resource[i]['memory']['usage'] = memory_usage
            cpus = machine['cpus']
            j = 0
            while j < len(cpus):
                cpu_usage = Redis.lrange(CPU_USAGE_PREFIX + hostname + ':' + str(j), -MONITOR_LIST_LEN, -1)
                fill_list(cpu_usage)
                cpus[j]['usage'] = cpu_usage
                j += 1
            cluster_resource[i]['cpus'] = cpus
            i += 1
        return jsonify(cluster=cluster_resource)
    except TypeError:
        return jsonify(success=False, message='No Monitor information at present.')


def fill_list(l):
    num = MONITOR_LIST_LEN - len(l)
    if num > 0:
        for i in range(num):
            l.append(0)
