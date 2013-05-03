import logging

LOGGER = logging.getLogger(__name__)

from cardiff.backends import base


class LoggerBackend(base.Backend):

    name = 'logger'

    def __init__(self, config, flush_interval):
        """Create a new backend object to emit stats with

        :param dict config: The backend specific configuration

        """
        super(LoggerBackend, self).__init__(config, flush_interval)

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
        :return:

        """
        self.log_counters(counters)
        self.log_gauges(gauges)
        self.log_sets(self.set_values(sets))
        self.log_timers(self.timer_values(timers))
        for key in int_counters.keys():
            for host in int_counters[key].keys():
                self.log_counters(int_counters[key][host], True)
        for key in int_gauges.keys():
            for host in int_gauges[key].keys():
                self.log_gauges(int_gauges[key][host], True)
        for key in int_timers.keys():
            for host in int_timers[key].keys():
                self.log_timers(self.timer_values(int_timers[key][host]), True)

    def log_counters(self, counters, internal=False):
        value = 'Counter %s=%s' if not internal else 'Internal Counter %s=%s'
        for key in counters:
            LOGGER.info(value, key, counters[key])

    def log_gauges(self, gauges, internal=False):
        value = 'Gauge %s=%s' if not internal else 'Internal Gauge %s=%s'
        for key in gauges:
            LOGGER.info(value, key, gauges[key])

    def log_sets(self, sets):
        for key in sets:
            for value in sets:
                LOGGER.info('Set %s %s=%s', key, value, sets[key][value])

    def log_timers(self, timers, internal=False):
        fmt = 'Timer %s %s=%s' if not internal else 'Internal Timer %s %s=%s'
        for key in timers:
            for value in timers[key]:
                LOGGER.info(fmt, key, value, timers[key][value])

