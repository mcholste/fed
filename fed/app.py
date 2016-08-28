import os
import sys
import logging
import importlib

import ujson as json
import tornado.ioloop
import tornado.web

DEFAULT_LISTEN_PORT = 8888

class Fed:
	def __init__(self, conf, loop, port=DEFAULT_LISTEN_PORT):
		self.log = logging.getLogger("Fed.app")
		if conf.has_key("listen_port"):
			port = conf["listen_port"]
		self.port = port
		self.loop = loop
		dynamically_loaded_modules = {}
		tornado_config = []
		for provider in conf.get("datasources", {}).keys():
			self.log.debug("Loading datasource %s" % provider)
			try:
				dynamically_loaded_modules["datasource." + provider] =\
					importlib.import_module("datasource." + provider)
				module_conf = {
					"path_prefix": provider,
					"loop": self.loop
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