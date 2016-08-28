import os
import sys
import logging
import datetime
import copy
from functools import partial
from time import time

import ujson as json

import tornado.ioloop
import tornado.web
from tornado.httpclient import AsyncHTTPClient, HTTPRequest

from . import BaseHandler

TORNADO_ROUTE = "(.+)"

class Handler(BaseHandler):
	def initialize(self, *args, **kwargs):
		super(Handler, self).initialize(*args, **kwargs)
		self.log = logging.getLogger("Fed.es")
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
		self.routes = {
			"federate": frozenset(["_search", "_msearch"])
		}
		self.timeout = 5

	def __init__(self, application, request, **kwargs):
		super(Handler, self).__init__(application, request, **kwargs)
	
	def is_federated(self):
		path = self.request.path
		if ".kibana" in path:
			return False
		for route in self.routes["federate"]:
			if route in path:
				# Parse this request
				return True

	def _merge_response(self, results, ret, proxy_node=None):
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
					# for field in ["doc_count_error_upper_bound", "sum_other_doc_count"]:
					# 	my_agg[field] += given_agg.get(field, 0)
					for given_bucket in given_agg["buckets"]:
						found = False
						for my_bucket in my_agg["buckets"]:
							if given_bucket["key"] == my_bucket["key"]:
								my_bucket["doc_count"] += given_bucket["doc_count"]
								found = True
								break
						if not found:
							my_agg["buckets"].append(given_bucket)

