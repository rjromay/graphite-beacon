from celery import Celery
import sys
from tornado import gen

from . import AbstractHandler, LOGGER


class CeleryHandler(AbstractHandler):

    name = 'cel'

    # Default options
    defaults = {
        'params': {},
        'app': 'celery',
    }


    def init_handler(self):
        self.task = self.options.get('task')
        assert self.task, 'Task name is not defined'
        self.module = self.options.get('module')
        assert self.module, 'Module name is not defined'
        self.broker = self.options.get('broker')
        assert self.broker, 'Broker not defined'
        self.app = self.options.get('app')

        celery_class = __import__(self.module, fromlist=[self.app])
        self.celery = getattr(celery_class, self.app)
        self.celery.conf.update(BROKER_URL = self.broker)

        mod = __import__(self.module, fromlist=[self.task])
        self.method = getattr(mod, self.task)
        

    @gen.coroutine
    def notify(self, level, alert, value, target=None, ntype=None, rule=None):
        LOGGER.debug("Handler (%s) %s", self.name, level)

        message = self.get_short(level, alert, value, target=target, ntype=ntype, rule=rule)
        data = {'alert': alert.name, 'desc': message, 'level': level}
        if target:
            data['target'] = target
        if rule:
            data['rule'] = rule['raw']
        #data.update(self.params)

        yield self.method.delay(data)
