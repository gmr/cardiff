import datetime
import flatdict
import logging
import rmqid
import time

flatdict.FlatDict.DELIMITER = '.'

LOGGER = logging.getLogger(__name__)

from cardiff.backends import base
from cardiff import controller


BACKEND = 'backend'
AMQP = 'amqp'
CONTROLLER = 'controller'
PLAINTEXT = 'plaintext'
PICKLE = 'pickle'

EXCEPTIONS = 'exceptions'
LAST_EXCEPTION = 'last_exception'
LAST_FLUSH = 'last_flush'
TIME_SPENT = 'prepare_time_ms'


class AMQPBackend(base.Backend):

    name = 'amqp'

    def __init__(self, config, flush_interval):
        """Create a new backend object to emit stats with

        :param dict config: The backend specific configuration

        """
        super(AMQPBackend, self).__init__(config, flush_interval)

        # Connection info
        self.url = self.amqp_uri
        self.exchange = self.config.get('exchange')

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
        LOGGER.info('Will push over AMQP to %s', self.url)

    @property
    def amqp_uri(self):
        if self.config.get('virtual_host') == '/':
            self.config['virtual_host'] = '%2F'
        return ('amqp://%(user)s:%(password)s@%(host)s:%(port)s/'
                '%(virtual_host)s' % self.config)

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
        :param dict int_timers: Internal timers
        :return:

        """
        start_time = time.time()
        timestamp = datetime.datetime.fromtimestamp(timestamp)

        # Calculate any timer values
        timer_values = self.flatten(self.timer_values(timers))

        with rmqid.Connection(self.amqp_uri) as conn:
            with conn.channel() as channel:
                self.send_counters(channel, timestamp, counters,
                                   self.counter_prefix)
                self.send_gauges(channel, timestamp, gauges,
                                 self.gauge_prefix)
                self.send_timers(channel, timestamp, timer_values,
                                 self.timer_prefix)
                self.send_internal_stats(channel, start_time, int_counters,
                                         int_gauges, int_timers, timestamp)


    def send_internal_stats(self, channel, start_time, counters,
                            gauges, timers, timestamp):
        """Send the internal cardiff stats to Graphite. By default this will be
        in the cardiff.graphite

        :param start_time:
        :param counters:
        :param gauges:
        :param timers:
        :rtype: None

        """
        last_flush = int(time.time())

        values = flatdict.FlatDict({AMQP: {EXCEPTIONS: self.exceptions}})
        counters[controller.METRICS_BACKEND][self.hostname] = values

        flat_timers = flatdict.FlatDict()
        for key in timers:
            flat_timers[key] = flatdict.FlatDict()
            for host in timers[key]:
                flat_timers[host] = self.timer_values(timers[key][host])

        values = flatdict.FlatDict({AMQP: {
            LAST_EXCEPTION: self.last_exception,
            LAST_FLUSH: last_flush,
            TIME_SPENT: (last_flush - start_time) * 1000
        }})

        gauges[controller.METRICS_BACKEND][self.hostname] = values

        self.send_counters(channel, timestamp, counters,
                           '%s.%s' % (controller.METRICS_INTERNAL,
                                      self.counter_prefix))
        self.send_gauges(channel, timestamp, gauges,
                         '%s.%s' % (controller.METRICS_INTERNAL,
                                    self.gauge_prefix))
        self.send_timers(channel, timestamp, flat_timers,
                         '%s.%s' % (controller.METRICS_INTERNAL,
                                    self.timer_prefix))
        self.exceptions = 0

    def get_rmqid_message(self, channel, metric_type, value, timestamp):
        """

        :param rmqid.Channel channel:
        :param str key:
        :param int or float value:
        :param datetime timestamp:
        :return: rmqid.Message

        """
        return rmqid.Message(channel, str(value),
                             {'app_id': 'cardiff',
                              'content-type': 'text/plain',
                              'timestamp': timestamp,
                              'message_type': metric_type})

    def flatten(self, values):
        """Return a nested dict as a flat dict.

        :param dict values: The nested dict to flatten
        :rtype: dict

        """
        return flatdict.FlatDict(values).as_dict()

    def key(self, prefix, key):
        """Return the properly formatted key for the given type prefix and
        main prefix.

        :param str prefix: The key prefix (data type)
        :param str key: The key
        :rtype: str

        """
        return '%s.%s.%s' % (self.prefix, prefix, key)

    def send_counters(self, channel, timestamp, counters, prefix):
        flat_values = flatdict.FlatDict(counters)
        for key in flat_values.keys():
            if flat_values[key] is None:
                flat_values[key] = 0
            routing_key = self.key(prefix, key)
            message = self.get_rmqid_message(channel,
                                             controller.METRICS_COUNTER,
                                             flat_values[key],
                                             timestamp)
            message.publish(self.exchange, routing_key)

    def send_gauges(self, channel, timestamp, gauges, prefix):
        flat_values = flatdict.FlatDict(gauges)
        for key in flat_values.keys():
            if flat_values[key] is None:
                flat_values[key] = 0
            routing_key = self.key(prefix, key)
            message = self.get_rmqid_message(channel,
                                             controller.METRICS_GAUGE,
                                             flat_values[key],
                                             timestamp)
            message.publish(self.exchange, routing_key)

    def send_timers(self, channel, timestamp, timers, prefix):
        for key in timers.keys():
            routing_key = self.key(prefix, key)
            message = self.get_rmqid_message(channel,
                                             controller.METRICS_COUNTER,
                                             timers[key],
                                             timestamp)
            message.publish(self.exchange, routing_key)
