from __future__ import print_function
import six
from ..api.data import Data
from .. import validations
from .. import sources
# Note: Invoke contents of sources at the func/method level so that it
# respects runtime switching between real and dummy sources.


class DataBroker(object):

    def __init__(self, configuration):
        if isinstance(configuration, six.string_types):
            beamline_id = validations.beamline_id(configuration)
        else:
            raise NotImplementedError("For now, DataBroker excepts a beamline "
                                      "name or alias. In future it may accept "
                                      "a more detailed configuration.")
        self.beamline_id = beamline_id

    def switch_beamlines(self, beamline_id):
        beamline_id = validations.beamline_id(beamline_id)
        self.beamline_id = beamline_id

    def search(self, start_time, end_time):
        "Get events from the MDS between these two times."
        find2 = sources.metadataStore.api.analysis.find2
        metadata = find2(start_time, end_time, beamline_id=self.beamline_id)
        print(metadata)
        return Data([])
