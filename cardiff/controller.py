import clihelper
import collections
import copy
from tornado import ioloop
import logging
import platform
import re
import resource
import socket
import threading
import time

from cardiff import backends
from cardiff import servers
from cardiff import __version__

LOGGER = logging.getLogger(__name__)

# Defaults
BACKLOG = 1024
HOST = '0.0.0.0'
STATSD_PORT = 8125
UPSTREAM_PORT = 8126
FLUSH_INTERVAL = 300

# Parsing stats
SAMPLE_RATE = re.compile(u'/^@([\d\.]+)/')
SIGNED_GAUGE = re.compile(u'/^[-+]/')

# Internal Stats constants
METRICS_BAD_STATS_RECEIVED = 'bad_lines_seen'
METRICS_BACKEND = 'backend'
METRICS_CONTROLLER = 'controller'
METRICS_COUNTER = 'counters'
METRICS_DELIVERY_TIME = 'delivery_time'
METRICS_GAUGE = 'gauges'
METRICS_HOST = 'host'
METRICS_INTERNAL = 'internal'
METRICS_PACKETS_RECEIVED = 'packets_received'
METRICS_PREFIX = 'cardiff'
METRICS_PROCESSING_TIME = 'processing_time'
METRICS_SET = 'sets'
METRICS_SNAPSHOT_TIME = 'snapshot_time'
METRICS_TIMER = 'timers'

METRICS_DOWNSTREAM_PACKETS_RECEIVED = 'downstream_packets_received'
METRICS_DOWNSTREAM_PAYLOADS_RECEIVED = 'downstream_payloads_received'
METRICS_BACKEND_DELIVERY_DURATION = 'delivery.%s.duration_ms'

# Key fixup regex
INVALID = re.compile(r'/[^a-zA-Z_\-0-9\.]/g')
SLASH = re.compile(r'/\//g')
WHITESPACE = re.compile(r'/\s+/g')
FIXUP = [(INVALID, ''),
         (SLASH, '-'),
         (WHITESPACE, '_')]


def hostname():
    """Return the hostname for the local machine"""
    return socket.gethostname().split('.')[0]


def merge_dicts(d, u):
    """Merge two nested dictionaries, stolen from
            http://stackoverflow.com/questions/3232943

    :param dict d: First dict to merge
    :param dict u: Second dict to merge
    :rtype: dict

    """
    for k, v in u.iteritems():
        if isinstance(v, collections.Mapping):
            r = merge_dicts(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d


class Cardiff(clihelper.Controller):
    """The core Cardiff controller/app responsible for processing metric
    values and then sending the values to backends for delivery.

    """
    def add_resource_usage(self):
        """Get the current resource usage and add it to the stats to be
        reported.

        """
        LOGGER.debug('Adding Resource usage')
        usage = resource.getrusage(resource.RUSAGE_SELF)
        self.internal_gauge('blocked_input', usage.ru_inblock)
        self.internal_gauge('blocked_output', usage.ru_oublock)
        self.internal_gauge('cpu_time_user', usage.ru_utime)
        self.internal_gauge('cpu_time_system', usage.ru_stime)
        ru_maxrss = usage.ru_maxrss
        if platform.system() in ['Linux']:
            ru_maxrss = ru_maxrss * 1024
        self.internal_gauge('memory_usage', ru_maxrss)
        self.internal_gauge('forced_context_switches', usage.ru_nivcsw)

    def cleanup(self):
        """Invoked when Cardiff is shutting down"""
        self.set_state(self.STATE_STOPPING)
        self.timer.stop()
        self.statsd_server.close()
        if self.ioloop._running:
            self.ioloop.stop()

    def create_empty_stat_attributes(self):
        """Create the attributes for carrying stats around"""
        self.counters = dict()
        self.gauges = dict()
        self.sets = dict()
        self.timers = dict()
        self.internal_counters = self.new_int_metric_dict()
        self.internal_gauges = self.new_int_metric_dict()
        self.internal_timers = self.new_int_metric_dict()

    def deliver_stats(self, backend, timestamp, counters, gauges, sets, timers,
                      int_counters, int_gauges, int_timers):
        """Deliver the stats to the various backend systems

        :type backend: cardiff.backends.base.Backend
        :param backend: The backend object to deliver to
        :param int timestamp: The timestamp for the metric snapshot
        :param dict counters: Counters to report
        :param dict gauges: Gauges to reports
        :param dict sets: Sets to report
        :param dict timers: Timers to report
        :param dict int_counters: Internal counters to report
        :param dict int_gauges: Internal gauges to report
        :param dict int_timers: Internal gauges to report

        """
        backend_start_time = time.time()
        LOGGER.debug('Delivering metrics to %s', backend.name)
        backend.deliver(timestamp,
                        copy.deepcopy(counters),
                        copy.deepcopy(gauges),
                        copy.deepcopy(sets),
                        copy.deepcopy(timers),
                        copy.deepcopy(int_counters),
                        copy.deepcopy(int_gauges),
                        copy.deepcopy(int_timers))

        self.internal_timer(METRICS_BACKEND_DELIVERY_DURATION %
                            backend.name, backend_start_time, METRICS_BACKEND)
        LOGGER.debug('Metrics delivered')

    def downstream_data(self, host, counters, gauges, sets, timers, internal):
        """Process downstream data adding the metrics in to internal values

        :param str host: The downstream host
        :param dict counters: Counter values
        :param dict gauges: Gauge values
        :param dict sets: Set values
        :param dict timers: Timer values
        :param dict internal: Internal values

        """
        start_time = time.time()
        self.internal_incr(METRICS_DOWNSTREAM_PAYLOADS_RECEIVED)

        # Process counter values
        LOGGER.debug('Processing %i downstream counter values', len(counters))
        for key in counters.keys():
            self.handle_counter(key, counters[key])
            self.internal_incr(METRICS_DOWNSTREAM_PACKETS_RECEIVED)

        # Process gauge values
        LOGGER.debug('Processing %i downstream gauge values', len(gauges))
        for key in gauges.keys():
            self.handle_gauge(key, gauges[key])
            self.internal_incr(METRICS_DOWNSTREAM_PACKETS_RECEIVED)

        # Process set values
        LOGGER.debug('Processing %i downstream set values', len(sets))
        for key in sets.keys():
            self.handle_set(key, sets[key])
            self.internal_incr(METRICS_DOWNSTREAM_PACKETS_RECEIVED)

        # Process timer values
        LOGGER.debug('Processing %i downstream timer values', len(timers))
        for key in timers.keys():
            if key not in self.timers:
                self.timers[key] = list()
            self.timers[key] += timers[key]
            self.internal_incr(METRICS_TIMER, len(timers[key]))
            self.internal_incr(METRICS_DOWNSTREAM_PACKETS_RECEIVED)

        # Set the internal metrics for the remote host
        LOGGER.debug('Merging downstream internal counts with own')
        self.internal_counters = merge_dicts(self.internal_counters,
                                             internal[METRICS_COUNTER])

        LOGGER.debug('Merging downstream internal gauges with own')
        self.internal_gauges = merge_dicts(self.internal_gauges,
                                           internal[METRICS_GAUGE])

        LOGGER.debug('Merging downstream internal timers with own')
        self.internal_timers = merge_dicts(self.internal_timers,
                                           internal[METRICS_TIMER])

        # Increment the processing time for metrics overall
        self.internal_timer(METRICS_PROCESSING_TIME, start_time)

    @property
    def flush_interval(self):
        """Return the flush interval from config or the default in seconds

        :rtype: int

        """
        return self.application_config.get('flush_interval') or FLUSH_INTERVAL

    def handle_counter(self, key, value=1, sample_size=1):
        """Handle counter packet data, incrementing the value.

        :param str key: The counter key
        :param (int or float) value: The counter value
        :param float sample_size: The sample size for the count

        """
        self.incr(key, int(value) * (1 / sample_size))
        self.internal_incr(METRICS_COUNTER)

    def handle_gauge(self, key, value=1):
        """Handle gauge packet data, incrementing the value if the value is
        signed, otherwise setting it to an absolute value.

        :param str key: The gauge key
        :param int value: The gauge value

        """
        if key not in self.gauges.keys():
            self.gauges[key] = 0

        if SIGNED_GAUGE.match(value):
            self.gauges[key] += int(value)
        else:
            self.gauges[key] = int(value)
        self.internal_incr(METRICS_GAUGE)

    def handle_set(self, key, value=1):
        """Increment the count of times this value has been added to the set

        :param str key: The set key
        :param (int or float) value: The set value

        """
        if key not in self.sets.keys():
            self.sets[key] = dict()
        try:
            self.sets[key][value] += 1
        except KeyError:
            self.sets[key] = {value: 1}
        self.internal_incr(METRICS_SET)

    def handle_timer(self, key, value=0, sample_size=1):
        """Append the timer value up to the size of the sample.

        :param str key: The timer key
        :param (int or float) value: The timer value
        :param float sample_size: The number of times the value was sampled

        """
        if sample_size < 1:
            sample_size = 1
        for iteration in range(0, int(sample_size)):
            try:
                self.timers[key].append(float(value))
            except KeyError:
                self.timers[key] = [float(value)]
        self.internal_incr(METRICS_TIMER)

    def incr(self, key, value=1):
        """Increment a counter by the value.

        :param str key: The key to increment
        :param int value: The value to increment by

        """
        try:
            self.counters[key] += value
        except KeyError:
            self.counters[key] = value

    def internal_gauge(self, name, value, metric_type=METRICS_CONTROLLER):
        """Set an internal gauge specified by key

        :param str key: The timer key
        :param int or float value: The value to set
        :param str metric_type: The metric type (controller, backend)

        """
        try:
            self.internal_gauges[metric_type][self.host][name] = value
        except KeyError:
            self.internal_gauges[metric_type][self.host] = dict()
            self.internal_gauge(name, value)

    def internal_incr(self, key, value=1, metric_type=METRICS_CONTROLLER):
        """Increment an internal counter specified by key

        :param str key: The timer key
        :param int or float value: The value to increment by
        :param str metric_type: The metric type (controller, backend)

        """
        try:
            self.internal_counters[metric_type][self.host][key] += value
        except KeyError:
            self.internal_counters[metric_type][self.host][key] = value

    def internal_timer(self, key, start_time, metric_type=METRICS_CONTROLLER):
        """Calculate the duration of now - start_time and append it to a timer
        specified by key.

        :param str key: The timer key
        :param float or int start_time: The start of the current timing value
        :param str metric_type: The metric type (controller, backend)

        """
        try:
            duration = (time.time() - start_time) * 1000
            self.internal_timers[metric_type][self.host][key].append(duration)
        except KeyError:
            self.internal_timers[metric_type][self.host][key] = list()
            self.internal_timer(key, start_time, metric_type)

    def new_int_metric_dict(self):
        """Return a new internal metric data structure for the current host.

        :rtype: dict

        """
        return {METRICS_BACKEND: {self.host: {}},
                METRICS_CONTROLLER: {self.host: {}}}

    def process_data(self, data):
        """Invoked by the UDP server to process an inbound UDP data packet,
        adding values to the correct data structure

        :param str data: Raw UDP data

        """
        start_time = time.time()
        self.internal_incr(METRICS_PACKETS_RECEIVED)

        # Handle multi-line stats
        if '\n' in data:
            LOGGER.debug('Processing multi-line: %r', data)
            return [self.process_data(value) for value in data.split('\n')]

        # Break apart the "frame"
        parts = data.split('|')

        # Break apart the
        bits = parts[0].split(':')
        key = bits.pop(0)
        for pattern, replacement in FIXUP:
            key = pattern.sub(replacement, key)

        # If there is no value with the key, default to 0
        if not bits:
            bits.append('1')
        value = bits[0] or 0

        # Validate sample-rate if it is passed
        sample = 1
        if len(parts) == 3:
            if not SAMPLE_RATE.match(parts[2]):
                LOGGER.warning('Bad line %r in msg %r has invalid sample rate',
                               parts, data)
                self.internal_timer(METRICS_PROCESSING_TIME, start_time)
                return self.internal_incr(METRICS_BAD_STATS_RECEIVED)
            sample = float(parts[2])

        # Handle the various stat types
        if parts[1] == 'c':
            self.handle_counter(key, value, sample)
        elif parts[1] == 'g':
            self.handle_gauge(key, value)
        elif parts[1] == 'ms':
            self.handle_timer(key, value, sample)
        elif parts[1] == 's':
            self.handle_set(key, value)
        else:
            self.internal_incr(METRICS_BAD_STATS_RECEIVED)
            LOGGER.warning('Bad line %r in msg %r', parts, data)

        self.internal_timer(METRICS_PROCESSING_TIME, start_time)

    def process_stats(self):
        # Tornado reconfigures the root logger on ioloop.IOLoop.start
        if self.tornado_logging_hack:
            self.tornado_logging_hack = False
            clihelper.setup_logging(self._debug)

        self.add_resource_usage()
        LOGGER.debug('Taking last interval snapshot')
        start_time = time.time()
        stats = self.snapshot()
        LOGGER.debug('Starting backend delivery threads')

        threads = []
        for backend in self.backends:
            args = [backend] + stats
            thread = threading.Thread(target=self.deliver_stats,
                                      args=tuple(args))
            thread.start()
            threads.append(thread)

        while threads:
            threads = [thread for thread in threads if thread.is_alive()]
            time.sleep(0.25)

        LOGGER.info('Completed stat delivery')
        self.internal_timer(METRICS_DELIVERY_TIME, start_time)

    def run(self):
        """Invoked by clihelper when the server is to start"""
        self.start_time = time.time()
        self.setup()
        try:
            self.ioloop.start()
        except KeyboardInterrupt:
            LOGGER.info('CTRL-C caught, shutting down')
            self.cleanup()

    def setup(self):
        """This method is called when the cli.run() method is invoked."""
        LOGGER.info('Cardiff version %s started', __version__)

        self.backends = backends.create(self.application_config.get('backends'),
                                        self.flush_interval)

        # Set the host that cardiff is running on for reporting
        self.host = hostname()

        # Setup the socket and listen
        self.ioloop = ioloop.IOLoop.instance()

        # Run the statsd server
        config = self.application_config.get('statsd')
        if config.get('enabled', True):
            self.statsd_server = servers.UDPServer(config.get('host', HOST),
                                                   config.get('port',
                                                              STATSD_PORT),
                                                   self.ioloop,
                                                   self.process_data)

        # Run the upstream server
        config = self.application_config.get('upstream')
        if config.get('enabled', False):
            logging_config = clihelper.LOGGING_OBJ
            self.upstream_server = servers.UpstreamServer(self.ioloop,
                                                          self.downstream_data,
                                                          logging_config)
            self.upstream_server.listen(config.get('port', UPSTREAM_PORT),
                                        config.get('host', HOST))

        # Default counters, gauges, sets and timers
        self.create_empty_stat_attributes()

        # Set the state
        self.set_state(self.STATE_ACTIVE)

        # Start the flush timer
        self.start_timer()

        # Set a flag to reset logs after tornado IOLoop has started
        self.tornado_logging_hack = True

    def snapshot(self):
        """Instead of trying to deal with changing stats data structures while
        we are delivering stats since we are not always guaranteed low latency
        delivery (such as to the cloud), make a snapshot of the various data
        structures and reset the main ones.

        :rtype: list

        """
        start_time = time.time()

        counters = dict()
        for key in self.counters.keys():
            counters[key] = self.counters.get(key)
            del self.counters[key]

        gauges = dict()
        for key in self.gauges.keys():
            gauges[key] = self.gauges.get(key)
            del self.gauges[key]

        sets = dict()
        for key in self.sets.keys():
            sets[key] = copy.deepcopy(self.sets[key])
            del self.sets[key]

        timers = dict()
        for key in self.timers.keys():
            timers[key] = copy.deepcopy(self.timers[key])
            del self.timers[key]

        int_counters = copy.deepcopy(self.internal_counters)
        self.internal_counters = self.new_int_metric_dict()

        int_gauges = copy.deepcopy(self.internal_gauges)
        self.internal_gauges = self.new_int_metric_dict()

        # Can't include the cost of snapshot timers if taking snapshot
        self.internal_timer(METRICS_SNAPSHOT_TIME, start_time)

        int_timers = copy.deepcopy(self.internal_timers)
        self.internal_timers = self.new_int_metric_dict()

        return [int(time.time()), counters, gauges, sets, timers,
                int_counters, int_gauges, int_timers]

    def start_timer(self):
        self.timer = ioloop.PeriodicCallback(self.process_stats,
                                             self.flush_interval * 1000)
        self.timer.start()


def main():
    clihelper.setup('cardiff', 'A python statsd clone', '1.0.0')
    clihelper.run(Cardiff)
