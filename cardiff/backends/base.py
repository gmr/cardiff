import logging
import math

from cardiff import controller

LOGGER = logging.getLogger(__name__)


class Backend(object):
    """Base backend class implements the contract with the controller and
    some methods to make consistent reporting of metrics easier.

    """
    name = 'base'

    def __init__(self, config, interval):
        """Create a new backend object to emit stats with

        :param dict config: The backend specific configuration

        """
        self.config = config
        self.interval = interval
        self.hostname = controller.hostname()
        self.exceptions = 0
        self.last_exception = 0

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
        raise NotImplementedError

    def calc_set_values(self, set_data):
        """Returns the dataset for a set that is reported to a backend

        :param  dict set_data: The set dataset
        :return: dict

        """
        if not set_data.keys():
            return {'count': 0, 'count_ps': 0}

        keys = set_data.keys().sort()
        hist_data = dict()
        for value in keys:
            key = '%i' % (value * 1000)
            if key not in hist_data:
                hist_data[key] = 0
            hist_data[key] += 1

        return {'count': len(keys),
                'count_ps': len(keys) / self.interval,
                'histogram': hist_data,
                'values': set_data}

    def calc_timer_values(self, timer):
        """Get the data payload for a specific timer value providing all the
        materialized calculations to dump into our destination.

        :param list timer: The timer values

        """
        if not len(timer):
            return {'count': 0,
                    'count_ps': 0,
                    'min': 0,
                    'max': 0,
                    'mean': 0,
                    'total': 0,
                    'median': 0,
                    '95th': 0,
                    '90th': 0}

        # Sort the values for the min/max/median/percentile values
        timer.sort()

        # Get the count and the sum of the values
        count = len(timer)
        total = sum(timer)

        #hist_data = dict()
        #for value in timer:
        #    key = '%i' % (value * 1000)
        #    try:
        #        hist_data[key] += 1
        #    except KeyError:
        #        hist_data[key] = 1

        return {'count': count,
                'count_ps': count / self.interval,
                #'histogram_ms': hist_data,
                'min': timer[0],
                'max': timer[-1],
                'mean': total / count,
                'total': total,
                'median': self.median(timer),
                '95th': self.percentile(timer, .95),
                '90th': self.percentile(timer, .90)}

    def median(self, values):
        """Calculate the median list value from a sorted list.

        :param list values: The sorted list values to get media value from
        :rtype: float

        """
        return self.percentile(values, 0.5)

    def percentile(self, values, percent):
        """Calculate the percentile from a sorted list.

        :param list values: The sorted list values to get media value from
        :param float percent: The percent value / 100
        :rtype: float

        """
        if not values:
            return None
        k = (len(values) - 1) * percent
        floor = math.floor(k)
        ceil = math.ceil(k)
        if floor == ceil:
            return values[int(k)]
        return (values[int(floor)] * (ceil-k)) + (values[int(ceil)] * (k-floor))

    def set_values(self, sets):
        """Calculate set values to show variations on

        :param dict sets: The dict of set values

        """
        calculated = dict()
        for key in sets:
            calculated[key] = self.calc_set_values(sets[key])
        return calculated

    def timer_values(self, timers):
        """Calculate timer values to show variations on

        :param dict timers: The dict of timer values

        """
        calculated = dict()
        for key in timers.keys():
            calculated[key] = self.calc_timer_values(timers[key])
        return calculated
