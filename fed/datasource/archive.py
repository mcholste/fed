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
		self.empty_results = []
		self.routes = {
			"federate": frozenset(["_search", "_msearch"])
		}
		self.timeout = 5

	def __init__(self, application, request, **kwargs):
		super(Handler, self).__init__(application, request, **kwargs)
	
	def _merge_response(self, results, ret, proxy_node=None):
		if results.has_key("error"):
			if proxy_node:
				ret["errors"][proxy_node] = results
			else:
				ret["errors"]["local"] = results
			return
		for result in results:
			ret.append(result)

