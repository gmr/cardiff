"""
upstream.py

"""
import logging
import pickle
import socket
import time

from cardiff.backends import base
from cardiff import controller

LOGGER = logging.getLogger(__name__)

BACKEND = 'backend'
CONTROLLER = 'controller'
EXCEPTIONS = 'exceptions'
LAST_EXCEPTION = 'last_exception_timestamp'
LAST_FLUSH = 'flush_timestamp'
UPSTREAM = 'upstream'


class UpstreamBackend(base.Backend):

    name = 'upstream'
    FRAME_END = 206

    def __init__(self, config, flush_interval):
        """Create a new backend object to emit stats to another Cardiff server

        :param dict config: The backend specific configuration

        """
        super(UpstreamBackend, self).__init__(config, flush_interval)
        self.host = config.get('host')
        self.port = config.get('port', 8127)
        LOGGER.info('Will push to a Cardiff Upstream at %s on port %i',
                    self.host, self.port)

    def connect(self):
        """Connect to the remote host"""
        LOGGER.debug('Connecting to %s:%i', self.host, self.port)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))

    def deliver(self, timestamp, counters, gauges, sets, timers,
                int_counters, int_gauges, int_timers):
        """Invoked by the core cardiff controller when there are stats to
        publish.

        :param float timestamp: The timestamp for the metrics
        :param dict counters: Counters to report
        :param dict gauges: Gauges to report
        :param dict sets: Sets to report
        :param dict timers: Timers to report
        :param dict int_counters: Internal counters
        :param dict int_gauges: Internal gauges
        :param dict int_timers: Internal timers

        """
        metrics = self.get_metrics(timestamp, counters, gauges, sets, timers,
                                   int_counters, int_gauges, int_timers)

        try:
            self.connect()
        except socket.error as error:
            LOGGER.error('Error sending stats upstream: %s', error)
            self.exceptions += 1
            self.last_exception = time.time()
            return

        LOGGER.info('Sending metrics upstream to %s:%s', self.host, self.port)
        try:
            self.socket.send(pickle.dumps(metrics) + chr(self.FRAME_END))
        except socket.error as error:
            LOGGER.error('Error sending stats upstream: %s', error)
            self.exceptions += 1
            self.last_exception = time.time()
        self.disconnect()

    def disconnect(self):
        """Disconnect from the remote host"""
        LOGGER.info('Disconnecting')
        self.socket.close()

    def get_metrics(self, timestamp, counters, gauges, sets, timers,
                    int_counters, int_gauges, int_timers):
        """Return a dict containing the properly structured values for upstream
        merging.

        :param float timestamp: The timestamp for the metrics
        :param dict counters: Counters to report
        :param dict gauges: Gauges to report
        :param dict sets: Sets to report
        :param dict timers: Timers to report
        :param dict int_counters: Internal counters
        :param dict int_gauges: Internal gauges
        :param dict int_timers: Internal timers
        :rtype: dict

        """
        values = {UPSTREAM: {EXCEPTIONS: self.exceptions}}
        int_counters[controller.METRICS_BACKEND][self.hostname] = values
        values = {UPSTREAM: {LAST_EXCEPTION: self.last_exception,
                             LAST_FLUSH: timestamp}}
        int_gauges[controller.METRICS_BACKEND][self.hostname] = values
        return {controller.METRICS_HOST: self.hostname,
                controller.METRICS_COUNTER: counters,
                controller.METRICS_GAUGE: self.sign_gauges(gauges),
                controller.METRICS_SET: sets,
                controller.METRICS_TIMER: timers,
                controller.METRICS_INTERNAL: {
                    controller.METRICS_COUNTER: int_counters,
                    controller.METRICS_GAUGE: int_gauges,
                    controller.METRICS_TIMER: int_timers}}

    def sign_gauges(self, values):
        """Sign the value and return it as a string.

        :param int or float value: The gauge value
        :rtype: str

        """
        output = dict()
        for key in values:
            if values[key] < 0:
                output[key] = '-%s' % values[key]
            elif values[key] > 0:
                output[key] = '+%s' % values[key]
            else:
                output[key] = '0'
        return output
