#!/usr/bin/env python

import threading
import elasticsearch
import json
import time
from datetime import datetime
import requests

def matchNode(properties,node):

    props = properties.split('.')
    propsSize = len(props)

    i=0
    while(i<propsSize):

        if(dict and type(node) is dict and props[i] ):

            if props[i] not in node:
                return None

            node = node[props[i]]
        else:
            return None
        i = i + 1

    return node

def convertProperties(prefix,obj):

    typeOfObj = type(obj)

    prefixWarp = ''
    if(not prefix == ''):
        prefixWarp = '%s.'%(prefix)

    data = []

    if(typeOfObj is dict):

        tree = obj
        size = len(tree)
        i = 0
        for item in tree:

            subResult = convertProperties(item,tree[item])

            if(type(subResult) is dict or type(subResult) is list):
                for result in subResult:
                    # print prefix,result
                    data.append('%s%s'%(prefixWarp,result))
            else:
                # print prefix,subResult
                data.append('%s%s'%(prefixWarp,subResult))


        return data

    if(typeOfObj is list):

        arr = obj
        for index in range(len(arr)):
            item = arr[index]

            subResult = convertProperties(prefix,item)

            if(type(subResult) is dict or type(subResult) is list):
                for result in subResult:
                    # print prefix,result
                    data.append('%s%s'%(prefixWarp,result))
            else:
                # print prefix,subResult
                data.append('%s%s'%(prefixWarp,subResult))

        return data

    return obj


class EsMetrics(threading.Thread):
    status_map = {
        'green': 0,
        'yellow': 1,
        'red': 2
    }

    def __init__(self, falcon_conf, es_conf):
        self.falcon_conf = falcon_conf
        self.es_conf = es_conf
        # Assign default conf
        if 'test_run' not in self.falcon_conf:
            self.falcon_conf['test_run'] = False
        if 'step' not in self.falcon_conf:
            self.falcon_conf['step'] = 60

        node_metrics_key_map = {
            'indices':{
                'search': ['query_total', 'query_time_in_millis', 'query_current', 'fetch_total', 'fetch_time_in_millis', 'fetch_current'],
                'indexing': ['index_total', 'index_current', 'index_time_in_millis', 'delete_total', 'delete_current', 'delete_time_in_millis'],
                'docs': ['count', 'deleted'],
                'store': ['size_in_bytes', 'throttle_time_in_millis'],
    	        'refresh': ['total','total_time_in_millis'],
    	        'flush': ['total','total_time_in_millis'],
                'fielddata':['memory_size_in_bytes','evictions']
            },
            'jvm':{
                'gc.collectors.young':['collection_count','collection_time_in_millis'],
                'gc.collectors.old':['collection_count','collection_time_in_millis'],
                'mem':['heap_used_percent','heap_committed_in_bytes']
            },
            'http':['current_open','total_opened'],
            'thread_pool':['bulk.queue','index.queue','search.queue','merge.queue','bulk.rejected','index.rejected','search.rejected','merge.rejected']
        }

        indice_metrics_key_map = {
            'primaries' : {
                'store':['size_in_bytes','throttle_time_in_millis']
            }

        }

        self.node_properties = convertProperties('',node_metrics_key_map)
        self.indice_properties = convertProperties('',indice_metrics_key_map)
        # self.index_metrics = {
        #     'search': ['query_total', 'query_time_in_millis', 'query_current', 'fetch_total', 'fetch_time_in_millis', 'fetch_current'],
        #     'indexing': ['index_total', 'index_current', 'index_time_in_millis', 'delete_total', 'delete_current', 'delete_time_in_millis'],
        #     'docs': ['count', 'deleted'],
        #     'store': ['size_in_bytes', 'throttle_time_in_millis'],
	    #     'refresh': ['total','total_time_in_millis'],
	    #     'flush': ['total','total_time_in_millis'],
        #     'jvm.mem': ['heap_used_percent']
        # }
        self.cluster_metrics = ['status', 'number_of_nodes', 'number_of_data_nodes', 'active_primary_shards', 'active_shards', 'unassigned_shards']
        self.counter_keywords = ['query_total', 'query_time_in_millis',
            'fetch_total', 'fetch_time_in_millis',
            'index_total', 'index_time_in_millis',
            'delete_total', 'delete_time_in_millis']
        super(EsMetrics, self).__init__(None, name=self.es_conf['endpoint'])
        self.setDaemon(False)

    def run(self):
        try:
            self.es = elasticsearch.Elasticsearch([self.es_conf['url']])
            falcon_metrics = []
            # Statistics
            timestamp = int(time.time())

            nodes_stats = self.es.nodes.stats()
            indices_stats = self.es.indices.stats()

            cluster_health = self.es.cluster.health()
            keyword_metric = {}

            # for indices status
            for indice in indices_stats['indices']:

                for propertie in self.indice_properties:

                    indice_property = 'indice[%s].%s'%(indice,propertie)
                    all_indice_property = 'indice[%s].%s'%('all',propertie)

                    if indice_property not in keyword_metric:
                        keyword_metric[indice_property] = 0

                    if all_indice_property not in keyword_metric:
                        keyword_metric[all_indice_property] = 0

                    print propertie

                    value = matchNode(propertie,indices_stats['indices'][indice])
                    if(value == None):
                        value = 0

                    keyword_metric[indice_property] = value
                    keyword_metric[all_indice_property] = keyword_metric[all_indice_property] + value

            # for nodes status
            for node in nodes_stats['nodes']:
                # index_stats = nodes_stats['nodes'][node]['indices']

                for propertie in self.node_properties:

                    node_property = 'node[%s].%s'%(node,propertie)
                    all_node_property = 'node[%s].%s'%('all',propertie)

                    if node_property not in keyword_metric:
                        keyword_metric[node_property] = 0

                    if all_node_property not in keyword_metric:
                        keyword_metric[all_node_property] = 0

                    value = matchNode(propertie,nodes_stats['nodes'][node])
                    if(value == None):
                        value = 0

                    keyword_metric[node_property] = value
                    keyword_metric[all_node_property] = keyword_metric[all_node_property] + value

            #     for type in self.index_metrics:
            #         for keyword in self.index_metrics[type]:
            #             full_metric_name = '%s.%s'%(type,keyword)
            #             if full_metric_name not in keyword_metric:
            #                 keyword_metric[full_metric_name] = 0
            #             keyword_metric[full_metric_name] += index_stats[type][keyword]
			# print '%s : %s'%(full_metric_name,keyword_metric[full_metric_name])
            for keyword in self.cluster_metrics:
                full_metric_name = '%s.%s'%('health',keyword)
                if keyword == 'status':
                    keyword_metric[full_metric_name] = self.status_map[cluster_health[keyword]]
                else:
                    keyword_metric[full_metric_name] = cluster_health[keyword]

            for keyword in keyword_metric:

                # print '%s : %s'%(keyword,keyword_metric[keyword])

                falcon_metric = {
                    'counterType': 'COUNTER' if keyword in self.counter_keywords else 'GAUGE',
                    'metric': "es." + keyword,
                    'endpoint': self.es_conf['endpoint'],
                    'timestamp': timestamp,
                    'step': self.falcon_conf['step'],
                    'tags': 'n=' + nodes_stats['cluster_name'],
                    'value': keyword_metric[keyword]
                }

                falcon_metrics.append(falcon_metric)

            # print 'falcon_metric : '
            # print json.dumps(falcon_metrics)

            if self.falcon_conf['test_run']:
                print json.dumps(falcon_metrics)
            else:
                req = requests.post(self.falcon_conf['push_url'], data=json.dumps(falcon_metrics))
                print datetime.now(), "INFO: [%s]" % self.es_conf['endpoint'], "[%s]" % self.falcon_conf['push_url'], req.text
        except Exception as e:

            if self.falcon_conf['test_run']:
                raise
            else:
                print datetime.now(), "ERROR: [%s]" % self.es_conf['endpoint'], e
