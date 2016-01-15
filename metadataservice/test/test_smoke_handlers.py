
from metadataservice.server.engine import DefaultHandler
from metadataservice.server.engine import RunStartHandler
from metadataservice.server.engine import EventDescriptorHandler
from metadataservice.server.engine import EventHandler
from metadataservice.server.engine import  RunStopHandler


if __name__ == "__main__":
    import nose
    nose.runmodule(argv=['-s', '--with-doctest'], exit=False)