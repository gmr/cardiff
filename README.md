Cardiff - a Python Statsd Clone
===============================
Cardiff is an extendible statsd clone written in Python that supports all
statsd metric types and a variety of configurable stats destinations.

Destination Types
-----------------
- graphite (Via plain text or pickle protocol)
- logging (Send data wherever you want, however you want)
- amqp
- cardiff (Upstream aggregation)
- statsd (Upstream aggregation via statsd protocol)
