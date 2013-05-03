import flatdict
import logging
import pickle
import socket
import struct
import time

from cardiff.backends import base
from cardiff import controller

LOGGER = logging.getLogger(__name__)

flatdict.FlatDict.DELIMITER = '.'

BATCH_SIZE = 300

BACKEND = 'backend'
GRAPHITE = 'graphite'
CONTROLLER = 'controller'
PLAINTEXT = 'plaintext'
PICKLE = 'pickle'

EXCEPTIONS = 'exceptions'
LAST_EXCEPTION = 'last_exception'
LAST_FLUSH = 'last_flush'
TIME_SPENT = 'prepare_time_ms'


class GraphiteBackend(base.Backend):
    """Publish metrics into graphite either via the plain text or pickle
    protocol

    """
    name = 'graphite'

    def __init__(self, config, flush_interval):
        """Create a new backend object to emit stats with

        :param dict config: The backend specific configuration

        """
        super(GraphiteBackend, self).__init__(config, flush_interval)

        # Format and Batch Size (used only for pickle format)
        self.format = config.get('format', PLAINTEXT)
        self.batch_size = config.get('batch_size', BATCH_SIZE)
        if self.format not in [PLAINTEXT, PICKLE]:
            LOGGER.info('Overwriting unsupported protocol %s with %s',
                        self.format, PLAINTEXT)
            self.format = PLAINTEXT

        # Connection info
        default_port = 2003 if self.format == PLAINTEXT else 2004
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', default_port)

        # Prefixes
        self.prefix = self.config.get('prefix', controller.METRICS_PREFIX)
        self.counter_prefix = self.config.get('counter_prefix',
                                              controller.METRICS_COUNTER)
        self.gauge_prefix = self.config.get('gauge_prefix',
                                            controller.METRICS_GAUGE)
        self.set_prefix = self.config.get('set_prefix',
                                          controller.METRICS_SET)
        self.timer_prefix = self.config.get('timer_prefix',
                                            controller.METRICS_TIMER)

        self.exceptions = 0
        self.last_exception = 0
        LOGGER.info('Will push to Carbon at %s on port %i in %s format %s',
                    self.host, self.port, self.format,
                    'using %i metric batches' % self.batch_size
                    if self.format == PICKLE else '')

    def connect(self):
        """Connect to the remote host"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))

    def deliver(self, timestamp, counters, gauges, sets, timers,
                int_counters, int_gauges, int_timers):
        """Invoked by the core cardiff controller when there are stats to
        publish.

        :param int timestamp: The timestamp for the metrics
        :param dict counters: Counters to report
        :param dict gauges: Gauges to report
        :param dict sets: Sets to report
        :param dict timers: Timers to report
        :param dict int_counters: Internal counters
        :param dict int_gauges: Internal gauges
        :param dict int_timers: Internal timers
        :return:

        """
        start_time = time.time()

        # Calculate any timer values
        timer_values = self.flatten(self.timer_values(timers))

        try:
            self.connect()
        except socket.error:
            self.exceptions += 1
            self.last_exception = int(time.time())
            return

        if self.format == PLAINTEXT:
            self.deliver_plaintext_values(timestamp,
                                          counters,
                                          self.counter_prefix)
            self.deliver_plaintext_values(timestamp, gauges, self.gauge_prefix)
            self.deliver_plaintext_values(timestamp,
                                          timer_values,
                                          self.timer_prefix)
        else:
            self.deliver_pickled_values(timestamp, counters,
                                        self.counter_prefix)
            self.deliver_pickled_values(timestamp, gauges, self.gauge_prefix)
            self.deliver_pickled_values(timestamp,
                                        timer_values,
                                        self.timer_prefix)

        # Get per-host timer values
        timer_values = flatdict.FlatDict()
        for key in int_timers:
            timer_values[key] = flatdict.FlatDict()
            for host in int_timers[key]:
                timer_values[host] = self.timer_values(int_timers[key][host])

        self.deliver_internal_stats(start_time,
                                    int_counters,
                                    int_gauges,
                                    timer_values.as_dict())

        self.disconnect()

    def flatten(self, values):
        """Return a nested dict as a flat dict.

        :param dict values: The nested dict to flatten
        :rtype: dict

        """
        return flatdict.FlatDict(values).as_dict()

    def deliver_internal_stats(self, start_time, counters, gauges, timers):
        """Send the internal cardiff stats to Graphite. By default this will be
        in the cardiff.graphite

        :param start_time:
        :param counters:
        :param gauges:
        :param timers:
        :rtype: None

        """
        last_flush = int(time.time())

        values = flatdict.FlatDict({GRAPHITE: {EXCEPTIONS: self.exceptions}})
        counters[controller.METRICS_BACKEND][self.host] = values

        values = flatdict.FlatDict({GRAPHITE: {
            LAST_EXCEPTION: self.last_exception,
            LAST_FLUSH: last_flush,
            TIME_SPENT: (last_flush - start_time) * 1000
        }})
        gauges[controller.METRICS_BACKEND][self.host] = values

        stats = flatdict.FlatDict({
            controller.METRICS_COUNTER: counters,
            controller.METRICS_GAUGE: gauges,
            controller.METRICS_TIMER: timers
        })

        if self.format == PLAINTEXT:
            self.deliver_plaintext_values(start_time, stats.as_dict(),
                                          controller.METRICS_INTERNAL)
        else:
            self.deliver_pickled_values(start_time, stats.as_dict(),
                                        controller.METRICS_INTERNAL)
        self.exceptions = 0

    def key(self, prefix, key):
        """Return the properly formatted key for the given type prefix and
        main prefix.

        :param str prefix: The key prefix (data type)
        :param str key: The key
        :rtype: str

        """
        return '%s.%s.%s' % (self.prefix, prefix, key)

    def deliver_plaintext_values(self, timestamp, values, prefix):
        """Send plaintext formatted counter data to graphite.

        :param int timestamp: The time for the metrics
        :param dict values: The values to send
        :param str prefix: The prefix for the key

        """
        flat_values = flatdict.FlatDict(values)
        flat_values.DELIMITER = '.'
        for key in flat_values.keys():
            if flat_values[key] is None:
                flat_values[key] = 0
            self.send('%s %s %s' % (self.key(prefix, key),
                                    flat_values[key],
                                    int(timestamp)))

    def deliver_pickled_values(self, timestamp, values, prefix):
        """Deliver values in the Python pickle format

        :param int timestamp: The time for the metrics
        :param dict values: The values to send
        :param str prefix: The prefix for the key

        """
        flat_values = flatdict.FlatDict(values)
        flat_values.DELIMITER = '.'
        metrics = list()
        for key in flat_values.keys():
            if flat_values[key] is None:
                flat_values[key] = 0
            metrics.append((self.key(prefix, key),
                            (int(timestamp),
                             flat_values[key])))

        while metrics:
            pickled = pickle.dumps(metrics[:self.batch_size], protocol=-1)
            self.socket.send(struct.pack('!L', len(pickled)) + pickled)
            if len(metrics) > self.batch_size:
                metrics = metrics[self.batch_size:]
            else:
                break

    def disconnect(self):
        """Disconnect from the remote host"""
        self.socket.close()

    def send(self, line):
        """Send the line to the Graphite server

        :param str line: The line to send to graphite

        """
        LOGGER.debug('Sending: %s', line)
        self.socket.send('%s\n' % line)
