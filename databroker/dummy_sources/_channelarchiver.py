"""
This module imitates the API of channelarchiver.
See github.com/NSLS-II/channelarchiver.

It uses as much of the actual channelarchiver as possible, aiming
to avoid surprises when switching from this dummy to the real thing.
"""
from channelarchiver import utils
from channelarchiver.models import ChannelData
import six
from datetime import datetime as dt  # noqa, used by eval()


class Archiver(object):

    def __init__(self, host):
        pass

    def get(self, channels, start, end, limit=100, interpolation='linear',
            scan_archives=True, archive_keys=None, tz=None):
        """
        channels : string
            a string representing a valid Python expression to be
            evaluted as '(values, times)'

            Example: '([1,2], [dt(2014, 1, 1), dt(2014, 1, 2)])'
        start : string or datetime
            Strings are interpreted as ISO timestamps.
        end : string or datetime
            Strings are interpreted as ISO timestamps.
        interpolation : string
            The databroker should be handling all the interpolation,
            so although this default value matches the ChannelArchiver
            default ('linear') that will raise an error here. All
            calls must specify interpolation='raw'.

        TODO: The other parameters are not yet implemented. They
        are merely ignored.
        """

        if interpolation != 'raw':
            raise NotImplementedError("We should not ask the Archiver "
                                      "to interpolate.")

        # If channels is not a list, make it a one-element list.
        received_str = isinstance(channels, six.string_types)
        if received_str:
            channels = [channels]

        if isinstance(start, six.string_types):
            start = utils.datetime_from_isoformat(start)
        if isinstance(end, six.string_types):
            end = utils.datetime_from_isoformat(end)

        if start.tzinfo is None:
            start = utils.localize_datetime(start, utils.local_tz)

        if end.tzinfo is None:
            end = utils.localize_datetime(end, utils.local_tz)

        result = []
        for channel in channels:
            values, times = eval(channel)
            result.append(ChannelData(values=values, times=times))

        if received_str:
            result = result[0]
        return result
