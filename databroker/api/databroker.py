import six
from .. import validations
from .. import sources

find2 = sources.metadataStore.api.analysis.find2


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
        metadata = find2(start_time, end_time, beamline_id=self.beamline_id)
