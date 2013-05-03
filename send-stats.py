"""
send-stats.py

"""
import logging
import random
import socket
import time
LOGGER = logging.getLogger(__name__)


while True:
    print 'Sending'
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('127.0.0.1', 8125))

    for iteration in range(random.randint(0, 200)):
        s.send('cardiff.test_counter_%i:%i|c' % (iteration, random.randint(0, 25)))

    for iteration in range(100):
        s.send('cardiff.test_gauge_%i:%i|g' % (iteration, random.randint(0, 100)))

    for iteration in range(100):
        for timing in range(random.randint(0, 100)):
            s.send('cardiff.test_timing_%i:%0.2f|ms' % (iteration,
                                                        float(float(random.randint(0, 10000)) / 1000)))

    s.close()
    print 'Sleeping'
    time.sleep(random.randint(0, 15))
