import logging
import socket

LOGGER = logging.getLogger(__name__)

from cardiff.backends import base


class StatsdBackend(base.Backend):

    name = 'statsd'

    def __init__(self, config, flush_interval):
        """Create a new backend object to emit stats with

        :param dict config: The backend specific configuration

        """
        super(StatsdBackend, self).__init__(config, flush_interval)

        self.exceptions = 0
        self.last_exception = 0

        # Connection info
        self.host = config.get('host')
        self.port = config.get('port', 8125)
        self.hostname = socket.gethostname().split('.')[0]

    def connect(self):
        """Connect to the remote host"""
        LOGGER.debug('Connecting to %s:%i', self.host, self.port)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
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
        LOGGER.info('Compiling metrics in statsd format')
        output = list()
        output += self.format_counters(counters)
        output += self.format_gauges(gauges)
        output += self.format_sets(sets)
        output += self.format_timers(timers)
        self.connect()
        LOGGER.info('Sending %i metrics upstream', len(output))
        for line in output:
            self.send(line)
        self.disconnect()

    def disconnect(self):
        """Disconnect from the remote host"""
        LOGGER.info('Disconnecting')
        self.socket.close()

    def format_counters(self, counters):
        return ['%s:%s|c' % (key, counters[key]) for key in counters.keys()]

    def format_timers(self, timers):
        output = list()
        for key in timers:
            datapoints = len(timers[key])
            mean_time = float(sum(timers[key])) / datapoints
            output.append('%s:%0.3f|ms|%i' % (key, mean_time, datapoints))
        return output

    def format_gauges(self, gauges):
        return ['%s:%s|g' % (key, gauges[key]) for key in gauges.keys()]

    def format_sets(self, sets):
        output = list()
        for key in sets:
            for item in sets[key]:
                output.append('%s:%s|s' % (key, item))
        return output

    def send(self, line):
        """Send the line to the Graphite server

        :param str line: The line to send to graphite

        """
        LOGGER.debug('Sending: %s', line)
        try:
            self.socket.send('%s' % line)
        except socket.error as error:
            LOGGER.error('Error sending stat: %s', error)
            self.disconnect()
            self.connect()
