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

def merge(src, dst):
	if dst == None:
		return src
	if type(src) == dict and type(dst) == dict:
		for k, v in src.iteritems():
			if type(v) is dict and dst.has_key(k):
				dst[k] = merge(v, dst[k])
			elif type(v) is list and dst.has_key(k):
				if len(v) == len(dst[k]):
					for i, item in enumerate(v):
						dst[k][i] = merge(item, dst[k][i])
				else:
					raise Exception("Cannot merge arrays of different length")
			elif type(v) is int or type(v) is float and dst.has_key(k):
				dst[k] += v
			else:
				dst[k] = v
	elif type(src) == int or type(src) == float:
		dst += src
	else:
		dst = src
	return dst

TORNADO_ROUTE = "(.+)"

class BaseHandler(tornado.web.RequestHandler):
	def initialize(self, path_prefix, 
		loop=tornado.ioloop.IOLoop.instance(), 
		local_nodes=[], 
		remote_nodes=[]):
		if not local_nodes + remote_nodes:
			raise Exception("No nodes configured.")
		self.path_prefix = path_prefix
		self.local_nodes = [ (x["host"], x["port"], x.get("protocol", "http")) for x in local_nodes ] \
			or [("127.0.0.1", DEFAULT_LISTEN_PORT)]
		self.remote_nodes = [ (x["host"], x["port"], x.get("protocol", "http")) for x in remote_nodes ]
		self.io_loop = loop
		self.client = AsyncHTTPClient(self.io_loop)
		self.results = None
		self.outstanding = {}
		self.empty_results = {}
		self.routes = {
			"federate": frozenset([])
		}
		self.passthrough_node = self._node_str(self.local_nodes[0])

	def __init__(self, application, request, **kwargs):
		super(BaseHandler, self).__init__(application, request, **kwargs)
	
	def _bad_request(self, error):
		self.set_status(400)
		self.write(json.dumps({"error": error}))
		self.finish()

	def passthrough(self, **kwargs):
		self.request.host = self.passthrough_node
		self.request.uri  = "/" + "/".join(self.request.uri.split("/")[2:])
		uri = self.request.full_url()
		req = HTTPRequest(uri,
			method=self.request.method, 
			body=self.request.body,
			headers=self.request.headers,
			follow_redirects=False,
			allow_nonstandard_methods=True
		)
		
		self.log.debug("Passing req through %r" % req.url)
		self.client.fetch(req, self.passthrough_callback, raise_error=False)

	def passthrough_callback(self, response):
		if (response.error and not
			isinstance(response.error, tornado.httpclient.HTTPError)):
			self.set_status(500)
			self.write('Internal server error:\n' + str(response.error))
		else:
			self.set_status(response.code, response.reason)
			self._headers = tornado.httputil.HTTPHeaders() # clear tornado default header

			for header, v in response.headers.get_all():
				if header not in ('Content-Length', 'Transfer-Encoding', 'Content-Encoding', 'Connection'):
					self.add_header(header, v) # some header appear multiple times, eg 'Set-Cookie'

			if response.body:                   
				self.set_header('Content-Length', len(response.body))
				self.write(response.body)
		self.finish()


	def federated_callback(self, node_str, response):
		del self.outstanding[node_str]
		if (response.error and not
			isinstance(response.error, tornado.httpclient.HTTPError)):
			self.log.error("Error back from %s: %s" % (node_str, response.error))
			self.callback()
		else:
			try:
				results = json.loads(response.body)
				if self.results:
					for i, response in enumerate(results["responses"]):
						self.results[i] = self._merge_response(response, self.results["responses"][i])
				else:
					self.results = results
				self.callback()
			except Exception as e:
				self.log.exception("Unable to decode response from node %s" % node_str, 
					exc_info=e)
				if not self.results:
					self.results = copy.deepcopy(self.empty_results)
				if not self.results.has_key("errors"):
					self.results["errors"] = {}
				self.results["errors"][node_str] = { "error": "Invalid results." }
				self.callback()

	# Base classes must override if they have particulars
	def is_federated(self):
		path = self.request.path
		for route in self.routes["federate"]:
			if route in path:
				# Parse this request
				return True

	def federate(self):
		for node in self.remote_nodes + self.local_nodes:
			path_prefix = ""
			if node in self.remote_nodes:
				path_prefix = self.path_prefix + "/"
			node_str = self._node_str(node)
			uri  = "/" + path_prefix + "/".join(self.request.uri.split("/")[2:])
			req = HTTPRequest("%s://%s%s" % (node[2], node_str, uri),
				method=self.request.method, 
				body=self.request.body,
				headers=self.request.headers,
				follow_redirects=False,
				allow_nonstandard_methods=True
			)
			self.outstanding[node_str] = time()
			self.log.debug("Federating req through %r" % req.url)
			self.client.fetch(req, partial(self.federated_callback, node_str), raise_error=False)

	@tornado.web.asynchronous
	def put(self, uri):
		self.post(uri)

	@tornado.web.asynchronous
	def head(self, uri):
		self.post(uri)

	@tornado.web.asynchronous
	def post(self, uri):
		# Unless we explicitly want to intercept and federate, pass the req through
		#  to the first node listed in local_nodes conf
		
		if self.is_federated():
			self.federate()
		else:
			self.passthrough()
			
	@tornado.web.asynchronous
	def get(self, uri):
		self.post(uri)
				
	def _node_str(self, node):
		return node[0] + ":" + str(node[1])

	def _finish(self):
		self.set_header("Content-Type", "application/json")
		self.write(json.dumps(self.results))
		self.finish()

	def callback(self):
		# TODO trim results
		if not self.outstanding:
			self._finish()
		else:
			for k in self.outstanding.keys():
				v = self.outstanding[k]
				time_outstanding = time() - v
				if time_outstanding >= self.timeout:
					self.log.error("Gave up waiting on %s after %f" % (k, time_outstanding))
					del self.outstanding[k]
					if not self.outstanding:
						self._finish()
				else:
					self.log.debug("Waiting on %s, has been %f" % (k, time_outstanding))


	# Subclass must implement a _merge_response
	def _merge_response(self, results, ret, proxy_node=None):
		if results.has_key("error"):
			if proxy_node:
				ret["errors"][proxy_node] = results
			else:
				ret["errors"]["local"] = results
			return
		return ret

