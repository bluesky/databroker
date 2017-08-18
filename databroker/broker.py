from warnings import warn

warn("The module databroker.broker is just a shim for backward-compatibility. "
     "You can import directly from databroker.")


from databroker._core import Broker, BrokerES, ALL
