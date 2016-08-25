import os
import sys
import logging
import datetime
import copy
from functools import partial
from time import time

import ujson as json
from elasticsearch import Elasticsearch
from elasticsearch.client import ClusterClient, IndicesClient
from elasticsearch.helpers import parallel_bulk
from elasticsearch.exceptions import TransportError

import tornado.ioloop
import tornado.web
from tornado.httpclient import AsyncHTTPClient, HTTPRequest

def forked_search(node, query, indices=""):
	es = Elasticsearch([{"host": node[0], "port": node[1]}])
	try:
		res = es.search(indices, body=json.dumps(query), explain=False)
		return res
	except Exception as e:
		if isinstance(e, TransportError):
			return {
				"error": e.error,
				"info": e.info,
				"status_code": e.status_code
			}
		return {
			"error": str(e)
		}

TORNADO_ROUTE = "search(/.+)*"

class Handler(tornado.web.RequestHandler):
	def initialize(self, thread_pool, path_prefix, local_nodes, remote_nodes=None):
		self.pool = thread_pool
		self.path_prefix = path_prefix
		self.log = logging.getLogger("fed")
		self.local_nodes = [ (x["host"], x["port"]) for x in local_nodes ] or [("127.0.0.1", DEFAULT_LISTEN_PORT)]
		self.remote_nodes = []
		if remote_nodes:
			self.remote_nodes = [ (x["host"], x["port"]) for x in remote_nodes ]
		self.io_loop = tornado.ioloop.IOLoop.instance()
		self.client = AsyncHTTPClient(self.io_loop)
		self.timeout = 5
		self.results = None
		self.outstanding = {}
		self.empty_results = {
			"errors": {},
			"_shards": {
				"total": 0,
				"failed": 0,
				"successful": 0
			},
			"timed_out": False,
			"took": 0,
			"hits": {
				"max_score": 0,
				"total": 0,
				"hits": []
			}
		}
		self.futures = []

	def __init__(self, application, request, **kwargs):
		super(Handler, self).__init__(application, request, **kwargs)
	
	def _bad_request(self, error):
		self.set_status(400)
		self.write(json.dumps({"error": error}))
		self.finish()

	@tornado.web.asynchronous
	def post(self, indices):
		self.log.debug("indices: %s" % indices)
		if indices:
			indices = indices[1:].split(",")
		try:
			query = json.loads(self.request.body)
			self._query(query, indices)
		except ValueError as e:
			self.log.exception("Invalid JSON: %s" % self.request.body, exc_info=e)
			self._bad_request("Invalid JSON")


	@tornado.web.asynchronous
	def get(self, indices):
		if indices:
			indices = indices[1:].split(",")
		query = {
			"query": {
				"match": {"_all": self.request.arguments.get("query", "")[0] }
			}
		}
		self._query(query, indices)

	def _query(self, query, indices=""):
		self.query = query
		self.log.debug("Query: %r" % query)
		result = None
		for node in self.local_nodes:
			self.outstanding[ self._node_str(node) ] = time()
			result = self.pool.apply_async(forked_search, (node, query, indices))
			self.futures.append((node, result))
		for node in self.remote_nodes:
			self.outstanding[ self._node_str(node) ] = time()
			self.proxy_query(node, query, indices, callback=partial(self.proxy_callback, node, query))
		
		ret = {}
		for node, result in self.futures:
			node_str = self._node_str(node)
			result = result.get(timeout=self.timeout)
			del self.outstanding[node_str]
			self.log.debug("Got result for node %s" % node_str)
			if isinstance(result, dict):
				self._add_results(result)
			else:
				result = { "error": str(result) }
				self._add_results(result)
			
		self.callback()
		
	def _node_str(self, node):
		return node[0] + ":" + str(node[1])

	def proxy_callback(self, node, query, result):
		self.log.debug("Result back from proxy node %s" % self._node_str(node))
		del self.outstanding[ self._node_str(node) ]
		# result is an HTTPResponse object
		try:
			results = json.loads(result.body)["results"]
			self._add_results(results, proxy_node=self._node_str(node))
			self.callback()
		except Exception as e:
			self.log.exception("Unable to decode response from node %s" % self._node_str(node), 
				exc_info=e)
			if not self.results:
				self.results = copy.deepcopy(self.empty_results)
			self.results["errors"][ self._node_str(node) ] = { "error": "Invalid results." }
			self.callback()

	def callback(self):
		# TODO trim results
		if not self.outstanding:
			self.set_header("Content-Type", "application/json")
			self.write(json.dumps({"query": self.query, "results": self.results}))
			self.finish()
		else:
			for k, v in self.outstanding.iteritems():
				time_outstanding = time() - v
				if time_outstanding >= self.timeout:
					self.log.error("Gave up waiting on %s after %f" % (k, time_outstanding))
					del self.outstanding[k]
				else:
					self.log.debug("Waiting on %s, has been %f" % (k, time_outstanding))

	def _add_results(self, results, proxy_node=None):
		if not self.results:
			self.results = copy.deepcopy(self.empty_results)
		# Reduce the amount of names resolved with dot operator in nested loops below
		ret = self.results
		
		if results.has_key("error"):
			if proxy_node:
				ret["errors"][proxy_node] = results
			else:
				ret["errors"]["local"] = results
			return
		for k in ret["_shards"]:
			ret["_shards"][k] += results["_shards"][k]
		if results["timed_out"]:
			ret["timed_out"] = True
			self.log.error("Node %s timed out" % (proxy_node))
		ret["took"] += results["took"]
		if results["hits"]["max_score"] > ret["hits"]["max_score"]:
			ret["hits"]["max_score"] = results["hits"]["max_score"]
		# Attach the proxy node name to hits if not existent
		if proxy_node:
			for hit in results["hits"]["hits"]:
				if not hit.has_key("_proxy_node"):
					hit["_proxy_node"] = proxy_node
		ret["hits"]["total"] += results["hits"]["total"]
		ret["hits"]["hits"].extend(results["hits"]["hits"])

		if results.has_key("aggregations"):
			if not ret.has_key("aggregations"):
				ret["aggregations"] = results["aggregations"]
				return
			for agg_name in results["aggregations"]:
				given_agg = results["aggregations"][agg_name]
				if not ret["aggregations"].has_key(agg_name):
					ret["aggregations"][agg_name] = given_agg
				else:
					my_agg = ret["aggregations"][agg_name]
					for field in ["doc_count_error_upper_bound", "sum_other_doc_count"]:
						my_agg[field] += given_agg[field]
					for given_bucket in given_agg["buckets"]:
						found = False
						for my_bucket in my_agg["buckets"]:
							if given_bucket["key"] == my_bucket["key"]:
								my_bucket["doc_count"] += given_bucket["doc_count"]
								found = True
								break
						if not found:
							my_agg["buckets"].append(given_bucket)

	
	def proxy_query(self, node, query, indices, callback=None):
		url = "http://%s:%d/%s/search" % (node[0], node[1], self.path_prefix)
		if indices:
			url = "http://%s:%d/%s/search/%s" % (node[0], node[1], self.path_prefix, ",".join(indices))
		request = HTTPRequest(url, method="POST", body=json.dumps(query))
		self.client.fetch(request, callback)


