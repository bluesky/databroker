# ######################################################################
# Copyright (c) 2014, Brookhaven Science Associates, Brookhaven        #
# National Laboratory. All rights reserved.                            #
#                                                                      #
# Redistribution and use in source and binary forms, with or without   #
# modification, are permitted provided that the following conditions   #
# are met:                                                             #
#                                                                      #
# * Redistributions of source code must retain the above copyright     #
#   notice, this list of conditions and the following disclaimer.      #
#                                                                      #
# * Redistributions in binary form must reproduce the above copyright  #
#   notice this list of conditions and the following disclaimer in     #
#   the documentation and/or other materials provided with the         #
#   distribution.                                                      #
#                                                                      #
# * Neither the name of the Brookhaven Science Associates, Brookhaven  #
#   National Laboratory nor the names of its contributors may be used  #
#   to endorse or promote products derived from this software without  #
#   specific prior written permission.                                 #
#                                                                      #
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS  #
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT    #
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS    #
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE       #
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,           #
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES   #
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR   #
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)   #
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,  #
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OTHERWISE) ARISING   #
# IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE   #
# POSSIBILITY OF SUCH DAMAGE.                                          #
########################################################################
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six

import getpass
from collections import namedtuple

__all__  = ['EventDescriptor', 'Event', 'Header', 'BeamLineConfig']


def _or_None(func):
    """
    Decorator to allow a validation function to also accept None
    """
    def inner(v):
        if v is None:
            return v
        return func(v)
    return inner


def enum_factory(valid):
    """
    A factory method to generate an enum validator.

    Parameters
    ----------
    valid : iterable
       iterable of valid values

    Returns
    -------
    validator : function
        v = validator(v) is a no-op if v is in valid
        and raise ValueError if not.

    """
    valid = set(valid)

    def inner(v):
        """
        This is a generated function, see `enum_factory`

        Parameters
        ----------
        v : object

        Returns
        -------
        v : object
           return the input if v is in the enum

        Raises
        ------
        ValueError
        """
        if v in valid:
            return v
        else:
            raise ValueError(
                "Value must be in {}, not {}".format(valid, v))
    return inner


validation_dict = {'str or None': _or_None(str),
                   'int': int,
                   'str': str,
                   "{'In Progress', 'Complete'}": enum_factory(
                       ['In Progress', 'Complete']),
                   'dict': dict,
                   'int or None': _or_None(int),
                   'dict or None': _or_None(dict),
                   'list': list,
                   '?': lambda x: x}
'''
Dictionary to map between strings and validation functions
'''


class entry_spec(namedtuple('entry_spec_base', ['name', 'ftype',
                                       'default_function', 'description'])):
    """
    Helper class for specifying the fields in mds entries

    Parameters
    ----------
    name : str
        The name of the field

    ftype : str
        The type of the value.  This is used to look up the validator function
        to be used (see `validation_dict`).

    default_function : callable or None
        If not None, called with no arguments to generate the default
        value.  If None, then the field does not have a default value
        and must be specified.

    description : str
       Prose description of the field
    """
    __slots__ = ()

    def __new__(cls, name, ftype, default_function, description):
        if ftype not in validation_dict:
            raise ValueError("ftype must be in validation dict. "
                             "you entered '{}', must be one of "
                             "{}".format(ftype, set(validation_dict)))
        if default_function is not None and not callable(default_function):
            raise ValueError('default function must be None or a callable')

        return super(entry_spec, cls).__new__(cls,
                                              name, ftype,
                                              default_function, description)


header_spec = (entry_spec('id', 'str or None', lambda: None, 'desc'),
               entry_spec('beamline_id', 'str', None, 'desc'),
               entry_spec('beamline_config_id', 'str', None, 'desc'),
               entry_spec('custom', 'dict or None', lambda: None, 'desc'),
               entry_spec('end_time', 'int or None', lambda: None, 'desc'),
               entry_spec('owner', 'str', getpass.getuser, 'desc'),
               entry_spec('scan_id', 'int', None, 'desc'),
               entry_spec('start_time', 'int', None, 'desc'),
               entry_spec('status', "{'In Progress', 'Complete'}",
                          lambda: 'In Progress', 'desc'))
"""
Field specification for run headers
"""

beamline_config_spec = (entry_spec('id', 'str or None', lambda: None, 'desc'),
                        entry_spec('headers', 'list', list, 'desc'),
                        entry_spec('beamline_id', 'str', None, 'desc'),
                        entry_spec('custom', 'dict or None',
                                   lambda: None, 'desc'))
"""
Field specification beam line configuration
"""

event_descriptor_spec = (entry_spec('id', 'str or None', lambda: None, 'desc'),
                         entry_spec('descriptor_name', 'str', None, 'desc'),
                         entry_spec('event_type_id', 'int', None, 'desc'),
                         entry_spec('run_header_id', 'str', None, 'desc'),
                         entry_spec('tag', 'list', list, 'desc'),
                         entry_spec('type_descriptor', '?',
                                    lambda: None, 'desc'),
                         entry_spec('data_keys', 'dict', None, 'desc'))
"""
Field specification for event descriptors
"""


event_spec = (entry_spec('id', 'str or None', lambda: None, 'desc'),
              entry_spec('data', 'dict', None, 'desc'),
              entry_spec('time', 'int', None, 'desc'),
              entry_spec('event_descriptor_id', 'str', None, 'desc'),
              entry_spec('run_header_id', 'str', None, 'desc'),
              entry_spec('owner', 'str', getpass.getuser, 'desc'),
              entry_spec('seq_no', 'int', lambda: 0, 'desc'))
"""
Field specification for events
"""


def _new_factory(names, spec_tuple, base_class):
    """
    Helper factory for generating named-tuple sub-classes mds entries

    This generates the `__new__` function which does the validation and
    provides the default values.

    Parameters
    ----------
    names : list
        names of the fields

    spec_tuple : tuple
        Tuple of entry_spec objects

    base_class : type
        The base-class object to build `__new__` on top of

    Returns
    -------
    new : function
        A `__new__` function to be used in _spec_tuple_factory
    """
    def __new__(cls, *args, **kwargs):
        args_dict = {k: v for k, v in zip(names, args)}
        for k in args_dict:
            if k in kwargs:
                raise TypeError('type object got multiples '
                                'values for keyword arguement {}'.format(k))

        args_dict.update(kwargs)

        for name, p_type, default, desc in spec_tuple:
            if name not in args_dict:
                if default is not None:
                    args_dict[name] = default()
                else:
                    raise ValueError("{} is a required arguement".format(name))

            # TODO put this in a try-catch block and do stack re-writing
            # to get @ericdill approved error messages
            validate = validation_dict[p_type]
            # validate everything, including the default values
            args_dict[name] = validate(args_dict[name])

        return base_class.__new__(cls, **args_dict)

    return __new__


def _spec_tuple_factory(spec_list, dict_name, cls_name):
    """
    A factory to generate namedtuple sub-classes to represent entries
    in MDS.

    Parameters
    ----------
    spec_list : iterable
        Iterable of entry_spec objects representing the fields

    dict_name : str
        Name of the entry type, used for documentation

    cls_name : str
        The name of the returned class

    Returns
    -------
    type
        A namedtuple subclass to help with MDS entries.

        Does basic validation and will fill in default values where
        possible.

    """
    _as_dict_doc_tmplate = """
    Returns the contents of the object as a dictionary suitable to pass
    to the {} function of metadataStore
    """

    _class_doc_template_param = """
    {p} : {t}{hd}
        {d}
    """

    _class_doc_template = """
    Class to hold data structure for {name}

    Parameters
    ----------
    {p}
    """

    spec_tuple = tuple(spec_list)
    names = tuple(s.name for s in spec_tuple)

    def as_dict(self):
        return dict(self._asdict())
    as_dict.__doc__ = _as_dict_doc_tmplate.format(dict_name)
    param = ''.join(_class_doc_template_param.format(
        p=p, t=t, d=d, hd=(', optional' if _ is not None else ''))
                              for p, t, _, d in spec_tuple)
    class_doc = _class_doc_template.format(name=dict_name,
                                           p=param)

    base_tuple = namedtuple(cls_name + 'Base', names)

    cls_new = _new_factory(names, spec_tuple, base_tuple)

    return type(cls_name, (base_tuple, ), {'__new__': cls_new,
                                           '__doc__': class_doc,
                                           '__slots__': (),
                                           'as_dict': as_dict})

Header = _spec_tuple_factory(header_spec, 'run_header', 'Header')
BeamLineConfig = _spec_tuple_factory(beamline_config_spec,
                                     'beamline_config',
                                     'BeamLineConfig')
EventDescriptor = _spec_tuple_factory(event_descriptor_spec,
                                      'event_descriptor',
                                      'EventDescriptor')
Event = _spec_tuple_factory(event_descriptor_spec,
                                      'event',
                                      'Event')
