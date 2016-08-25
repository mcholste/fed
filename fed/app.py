import os
import sys
import logging
import importlib
from multiprocessing.pool import ThreadPool

import ujson as json
import tornado.ioloop
import tornado.web

DEFAULT_LISTEN_PORT = 8888
DEFAULT_THREADS = 4

class Fed:
	def __init__(self, conf, loop, port=DEFAULT_LISTEN_PORT, threads=DEFAULT_THREADS):
		self.log = logging.getLogger("Fed.app")
		if conf.has_key("listen_port"):
			port = conf["listen_port"]
		self.port = port
		self.loop = loop
		self.pool = ThreadPool(processes=threads)
		dynamically_loaded_modules = {}
		tornado_config = []
		for provider in conf.get("datasources", {}).keys():
			log.debug("Loading datasource %s" % provider)
			try:
				dynamically_loaded_modules["datasource." + provider] =\
					importlib.import_module("datasource." + provider)
				module_conf = {
					"thread_pool": self.pool,
					"path_prefix": provider
				}
				for k, v in conf["datasources"][provider].iteritems():
					module_conf[k] = v
				tornado_config.append((
					"/%s/%s" % (provider, getattr(dynamically_loaded_modules["datasource." + provider], "TORNADO_ROUTE")),
					getattr(dynamically_loaded_modules["datasource." + provider], "Handler"),
					module_conf
				))
			except Exception as e:
				self.log.exception("Unable to import %s" % provider, exc_info=e)
				del conf["datasources"][provider]
				for route in conf["routes"].keys():
					if provider == conf["routes"][route]:
						self.log.warn("Invaliding route %s because module failed to load." % provider)
						del conf["routes"][route]
			
		self.application = tornado.web.Application(tornado_config)
		
	def start(self):
		self.application.listen(self.port)
		self.loop.start()


if __name__ == "__main__":
	logging.basicConfig()
	log = logging.getLogger()
	log.setLevel(logging.DEBUG)
	conf = json.load(open(sys.argv[1]))
	app = Fed(conf, tornado.ioloop.IOLoop.instance())
	app.start()