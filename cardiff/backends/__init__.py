"""Import backends dynamically from configuration"""

import logging

LOGGER = logging.getLogger(__name__)


def create(config, flush_interval):
    """Create the backends for delivery of stats data.

    :param dict config: backends configuration
    :rtype: list

    """
    backends = list()

    amqp_config = config.get('amqp', dict())
    if amqp_config.get('enabled', False):
        LOGGER.info('Creating AMQPBackend')
        from cardiff.backends import amqp
        backends.append(amqp.AMQPBackend(amqp_config, flush_interval))

    graphite_config = config.get('graphite', dict())
    if graphite_config.get('enabled', False):
        LOGGER.info('Creating GraphiteBackend')
        from cardiff.backends import graphite
        backends.append(graphite.GraphiteBackend(graphite_config,
                                                 flush_interval))

    logger_config = config.get('logger', dict())
    if logger_config.get('enabled', False):
        LOGGER.info('Creating LoggerBackend')
        from cardiff.backends import logger
        backends.append(logger.LoggerBackend(logger_config, flush_interval))

    statsd_config = config.get('statsd', dict())
    if statsd_config.get('enabled', False):
        LOGGER.info('Creating StatsdBackend')
        from cardiff.backends import statsd
        backends.append(statsd.StatsdBackend(statsd_config, flush_interval))

    upstream_config = config.get('upstream', dict())
    if upstream_config.get('enabled', False):
        LOGGER.info('Creating UpstreamBackend')
        from cardiff.backends import upstream
        backends.append(upstream.UpstreamBackend(upstream_config,
                                                 flush_interval))

    return backends
