import logging
import socket
import pickle
from tornado import iostream
from tornado import stack_context
from tornado import tcpserver


LOGGER = logging.getLogger(__name__)


class UDPServer(object):

    def __init__(self, host, port, ioloop, on_read_callback):
        self.ioloop = ioloop
        self.listen(host, port)
        self.on_read_callback = on_read_callback

    def close(self):
        self.ioloop.remove_handler(self.socket)
        self.socket.close()

    def listen(self, host, port):
        LOGGER.info('Listening on %s:%i UDP', host, port)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((host, port))
        self.ioloop.add_handler(self.socket.fileno(),
                                self.on_ioloop_events,
                                self.ioloop.READ | self.ioloop.ERROR)

    def on_ioloop_events(self, fd, events, error=None):
        """Handle IO/Event loop events, processing them.

        :param int fd: The file descriptor for the events
        :param int events: Events from the IO/Event loop
        :param int error: Was an error specified

        """
        if not fd:
            LOGGER.error('Received events on closed socket: %d', fd)
            return

        if events & self.ioloop.READ:
            self.read_from_socket()

        if events & self.ioloop.ERROR:
            LOGGER.error('Error event %r, %r', events, error)
            self.on_socket_error(error)

    def on_socket_error(self, error):
        if 'timed out' in str(error):
            raise socket.timeout
        if not error:
            return
        LOGGER.error('Socket error: %s', error[0], error[1])

    def read_from_socket(self):
        try:
            data = self.socket.recv(8192)
        except socket.timeout:
            LOGGER.error('Socket timeout, dying')
            return self.ioloop.stop()
        except socket.error, error:
            return self.on_socket_error(error)
        if data:
            self.on_read_callback(data)


class UpstreamConnection(object):

    FRAME_END = 206

    def __init__(self, stream, address, request_callback):
        self.stream = stream
        self.address = address
        self.request_callback = request_callback
        self.on_data_context = stack_context.wrap(self.on_data)
        try:
            self.stream.read_until(chr(self.FRAME_END), self.on_data_context)
        except iostream.StreamClosedError:
            self.close()

    def close(self):
        self.stream.close()

    def on_data(self, data):
        LOGGER.info('Received %i bytes from %s', len(data), self.address)
        self.close()
        self.request_callback(**pickle.loads(data))


class UpstreamServer(tcpserver.TCPServer):

    def __init__(self, ioloop, on_read_callback, logging_config):
        self.on_read_callback = on_read_callback
        super(UpstreamServer, self).__init__(io_loop=ioloop)
        self.logging_config = logging_config
        self.tornado_hack = True

    def handle_stream(self, stream, address):
        if self.tornado_hack:
            self.tornado_hack = False
            self.logging_config.configure()
        UpstreamConnection(stream, address, self.on_read_callback)

    def listen(self, port, address=""):
        LOGGER.info('Listening on %s:%i TCP', address, port)
        super(UpstreamServer, self).listen(port, address)
