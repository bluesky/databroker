from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from databroker.eventsource.tests.utils import (get_config_for_archiver,
                                                get_header_for_archiver,
                                                build_es_backed_archiver)

import logging
from pathlib import Path
import vcr as _vcr

cassette_library_dir = str(Path(__file__).parent / Path('cassettes'))

vcr = _vcr.VCR(
        serializer='json',
        cassette_library_dir=cassette_library_dir,
        record_mode='once',
        match_on=['method'],
)

# you need to initialize logging, otherwise you will not see anything
# from vcrpy
logging.basicConfig()
vcr_log = logging.getLogger("vcr")
vcr_log.setLevel(logging.INFO)


def test_name():
    config = get_config_for_archiver()
    aes = build_es_backed_archiver(config)
    assert aes.name == config['name']


def test_stream_names_given_header():
    config = get_config_for_archiver()
    aes = build_es_backed_archiver(config)
    hdr = get_header_for_archiver()
    assert aes.stream_names_given_header(hdr) == list(config['pvs'].keys())


@vcr.use_cassette()
def test_table_given_header():
    config = get_config_for_archiver()
    aes = build_es_backed_archiver(config)
    hdr = get_header_for_archiver()
    pvs = aes.stream_names_given_header(hdr)
    df = aes.table_given_header(hdr, stream_name=pvs[0])
    values = df[pvs[0]].values.tolist()
    assert len(values) == 61
