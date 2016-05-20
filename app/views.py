from flask import render_template, redirect, jsonify
from flask import Flask
from redis_utils import *
import json
from config import MONITOR_LIST_LEN
from app import app

@app.route('/')
@app.route('/index')
def index():
	return render_template('/index.html')

@app.route('/volume/remove')
def volume_remove():
	return jsonify(result = "success")

# page 3: performance monitor
@app.route('/monitor/info')
def get_monitor_info():
    cluster_resource = Redis.get(CLUSTER_RESOURCE)
    print cluster_resource
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


def fill_list(l):
    num = MONITOR_LIST_LEN - len(l)
    if num > 0:
        for i in range(num):
            l.append(0)