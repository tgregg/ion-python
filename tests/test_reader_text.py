# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at:
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
# OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the
# License.

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from itertools import chain

from amazon.ion.exceptions import IonException
from amazon.ion.reader import ReadEventType
from amazon.ion.reader_text import reader
from tests import listify, parametrize
from tests.event_aliases import *
from tests.reader_util import ReaderParameter, reader_scaffold

_b = bytearray
_P = ReaderParameter

# This is an encoding of a single top-level value and the expected events with ``NEXT``.
_TOP_LEVEL_VALUES = (
    (b'null', e_null()),

    (b'false', e_bool(False)),
    (b'true', e_bool(True)),
    (b'null.bool', e_bool()),

    (b'null.int', e_int()),
    (b'0', e_int(_b(b'0'))),
    (b'0xfe', e_int(_b(b'0xfe'))),
    (b'0b101', e_int(_b(b'0b101'))),
    (b'0b10_1', e_int(_b(b'0b101'))),
    (b'-0b101', e_int(_b(b'-0b101'))),
    (b'-0b10_1', e_int(_b(b'-0b101'))),
    (b'1', e_int(_b(b'1'))),
    (b'-1', e_int(_b(b'-1'))),
    (b'0xc1c2', e_int(_b(b'0xc1c2'))),
    (b'0xc1_c2', e_int(_b(b'0xc1c2'))),
    (b'-0xc1c2', e_int(_b(b'-0xc1c2'))),
    (b'-0xc1_c2', e_int(_b(b'-0xc1c2'))),

    (b'null.float', e_float()),
    (b'0.0e1', e_float(_b(b'0.0e1'))),
    (b'-0.0e-1', e_float(_b(b'-0.0e-1'))),
    # TODO +inf, -inf, nan

    (b'null.decimal', e_decimal()),
    (b'0.0', e_decimal(_b(b'0.0'))),
    (b'-0.0', e_decimal(_b(b'-0.0'))),
    (b'0d-1000', e_decimal(_b(b'0d-1000'))),
    (b'0d1000', e_decimal(_b(b'0d1000'))),
    (b'1d1', e_decimal(_b(b'1d1'))),
    (b'1234d-20', e_decimal(_b(b'1234d-20'))),
    (b'1d0', e_decimal(_b(b'1d0'))),
    (b'1d-1', e_decimal(_b(b'1d-1'))),
    (b'0d-1', e_decimal(_b(b'0d-1'))),
    (b'0d1', e_decimal(_b(b'0d1'))),
    (b'-1d1', e_decimal(_b(b'-1d1'))),
    (b'-1d0', e_decimal(_b(b'-1d0'))),
    (b'-1d-1', e_decimal(_b(b'-1d-1'))),
    (b'-0d-1', e_decimal(_b(b'-0d-1'))),
    (b'-0d1', e_decimal(_b(b'-0d1'))),

    (b'null.timestamp', e_timestamp()),
    (b'2007-01T', e_timestamp(_b(b'2007-01T'))),
    (b'2007T', e_timestamp(_b(b'2007T'))),
    (b'2007-01-01', e_timestamp(_b(b'2007-01-01'))),
    (b'2000-01-01T00:00:00.000Z', e_timestamp(_b(b'2000-01-01T00:00:00.000Z'))),
    (b'2000-01-01T00:00:00.000-00:00', e_timestamp(_b(b'2000-01-01T00:00:00.000-00:00'))),
    (b'2007-02-23T00:00+00:00', e_timestamp(_b(b'2007-02-23T00:00+00:00'))),
    (b'2007-01-01T', e_timestamp(_b(b'2007-01-01T'))),
    (b'2000-01-01T00:00:00Z', e_timestamp(_b(b'2000-01-01T00:00:00Z'))),
    (b'2007-02-23T00:00:00-00:00', e_timestamp(_b(b'2007-02-23T00:00:00-00:00'))),
    (b'2007-02-23T12:14:33.079-08:00', e_timestamp(_b(b'2007-02-23T12:14:33.079-08:00'))),
    (b'2007-02-23T20:14:33.079Z', e_timestamp(_b(b'2007-02-23T20:14:33.079Z'))),
    (b'2007-02-23T20:14:33.079+00:00', e_timestamp(_b(b'2007-02-23T20:14:33.079+00:00'))),
    (b'0001T', e_timestamp(_b(b'0001T'))),
    (b'0007-01T', e_timestamp(_b(b'0007-01T'))),
    (b'0007-01-01T', e_timestamp(_b(b'0007-01-01T'))),
    (b'0007-01-01', e_timestamp(_b(b'0007-01-01'))),
    (b'0000-01-01T00:00:00Z', e_timestamp(_b(b'0000-01-01T00:00:00Z'))),

    (b'null.symbol', e_symbol()),

    (b'null.string', e_string()),
    (b'" "', e_string(_b(b' '))),
    # TODO escape sequences

    (b'null.clob', e_clob()),
    (b'{{""}}', e_clob(_b(b''))),
    (b'{{ "abcd" }}', e_clob(_b(b'abcd'))),
    (b'{{"abcd"}}', e_clob(_b(b'abcd'))),
    (b'{{"abcd"\n}}', e_clob(_b(b'abcd'))),
    (b'{{\'\'\'ab\'\'\' \'\'\'cd\'\'\'}}', e_clob(_b(b'abcd'))),
    (b'{{\'\'\'ab\'\'\'\n\'\'\'cd\'\'\'}}', e_clob(_b(b'abcd'))),

    (b'null.blob', e_blob()),
    (b'{{}}', e_blob(_b(b''))),
    (b'{{ abcd }}', e_blob(_b(b'abcd'))),
    (b'{{ ab\ncd }}', e_blob(_b(b'abcd'))),

    (b'null.list', e_null_list()),

    (b'null.sexp', e_null_sexp()),

    (b'null.struct', e_null_struct()),

    # TODO standalone comments (probably can't go here because of annotations test)
)

_MULTI_EVENT_TOP_LEVEL_VALUES = (
    # TODO inputs with multiple output events don't work with the containerizer scaffold
    (b'nul 0', e_symbol(_b(b'nul')), e_int(_b(b'0'))),  # Ending with a symbol yields incomplete. This forces end.
    (b'$foo 0', e_symbol(_b(b'$foo')), e_int(_b(b'0'))),
    (b'\'a b\' 0', e_symbol(_b(b'a b')), e_int(_b(b'0'))),
    (b'\'\' 0', e_symbol(_b(b'')), e_int(_b(b'0'))),


    (b'\'\'\'foo\'\'\' \'\'\'\'\'\' \'\'\'""\'\'\' 0', e_string(_b(b'foo""')), e_int(_b(b'0'))),

    (b'[]', e_start_list(), e_end_list()),

    (b'()', e_start_sexp(), e_end_sexp()),

    (b'{}', e_start_struct(), e_end_struct()),
)

_ALL_TOP_LEVEL_VALUES = _TOP_LEVEL_VALUES + _MULTI_EVENT_TOP_LEVEL_VALUES


def _top_level_event_pairs(data, events, postpend):
    # TODO duplicated in test_reader_binary -- consolidate in reader_util
    first = True
    for event in events:
        input_event = NEXT
        if first:
            input_event = e_read(data + postpend)  # TODO added this postpending of space to get complete data events
            first = False
        yield input_event, event


def _top_level_iter(postpend=b' '):
    # TODO duplicated in test_reader_binary -- consolidate in reader_util
    for seq in _TOP_LEVEL_VALUES:
        data = seq[0]
        event_pairs = list(_top_level_event_pairs(data, seq[1:], postpend))
        yield data, event_pairs


# TODO how to consolidate this pythonically with the previous method?
def _all_top_level_iter(postpend):
    # TODO duplicated in test_reader_binary -- consolidate in reader_util
    for seq in _ALL_TOP_LEVEL_VALUES:
        data = seq[0]
        event_pairs = list(_top_level_event_pairs(data, seq[1:], postpend))
        yield data, event_pairs


def _scalar_params(delimiter=b''):
    for data, event_pairs in _top_level_iter(delimiter):
        yield _P(
            desc=data,
            event_pairs=event_pairs + [(NEXT, INC)]
        )


def _top_level_value_params(postpend=b' '):
    # TODO duplicated in test_reader_binary -- consolidate in reader_util
    """Converts the top-level tuple list into parameters with appropriate ``NEXT`` inputs.

    The expectation is starting from an end of stream top-level context.
    """
    for data, event_pairs in _all_top_level_iter(postpend):
        _, first = event_pairs[0]
        yield _P(
            desc='TL %s - %s - %r' % \
                 (first.event_type.name, first.ion_type.name, first.value),
            event_pairs=[(NEXT, END)] + event_pairs + [(NEXT, END)],
        )


def _all_scalars_in_one_container_params(delimiter):
    # TODO duplicated in test_reader_binary -- consolidate in reader_util
    @listify
    def generate_event_pairs():
        for data, event_pairs in _top_level_iter(delimiter):
            for event_pair in event_pairs:
                yield event_pair
                yield (NEXT, INC)

    yield _P(
        desc='ALL',
        event_pairs=generate_event_pairs()
    )


def _all_top_level_as_one_stream_params(postpend=b' '):
    # TODO duplicated in test_reader_binary -- consolidate in reader_util
    @listify
    def generate_event_pairs():
        yield (NEXT, END)
        for data, event_pairs in _all_top_level_iter(postpend):
            for event_pair in event_pairs:
                yield event_pair
            yield (NEXT, END)

    yield _P(
        desc='TOP LEVEL ALL',
        event_pairs=generate_event_pairs()
    )

_TEST_SYMBOLS = (
    (
        b'foo',
        b'$foo',
        b'\'a b\'',
        b'foo ',
        b'\'a b\' ',
        b'foo/*bar*/',
        b'\'a b\' //bar\n'
    ),
    (
        b'foo',
        b'$foo',
        b'a b',
        b'foo',
        b'a b',
        b'foo',
        b'a b',
    )
)


def _annotate_params(params):
    """Adds annotation wrappers for a given iterator of parameters,

    The requirement is that the given parameters completely encapsulate a single value.
    """
    assert len(_TEST_SYMBOLS[0]) == len(_TEST_SYMBOLS[1])
    params_list = list(params)
    for i in range(1, len(_TEST_SYMBOLS[0]) + 1):
        test_annotations = _TEST_SYMBOLS[0][0:i]
        expected_annotations = _TEST_SYMBOLS[1][0:i]
        for param in params_list:
            @listify
            def annotated():
                for input_event, output_event in param.event_pairs:
                    if input_event.type is ReadEventType.DATA:
                        data = b''
                        for test_annotation in test_annotations:
                            data += test_annotation + b'::'
                        data += input_event.data
                        input_event = read_data_event(data)
                        output_event = output_event.derive_annotations(expected_annotations)
                    yield input_event, output_event

            yield _P(
                desc='ANN %s' % param.desc,
                event_pairs=annotated(),
            )


def _generate_field_name():
    i = 0
    num_symbols = len(_TEST_SYMBOLS[0])
    while True:
        yield _TEST_SYMBOLS[0][i], _TEST_SYMBOLS[1][i]
        i += 1
        if i == num_symbols:
            i = 0


def _containerize_params(nested_params, with_skip=True):
    """Adds container wrappers for a given iteration of parameters.

    The requirement is that each parameter is a self-contained single value.
    """
    # TODO skipping
    name_generator = _generate_field_name()
    for info in ((IonType.LIST, b'[', b']', b','),
                 (IonType.SEXP, b'(', b')', b' '),  # TODO sexps without space delimiters are tested separately
                 (IonType.STRUCT, b'{ ', b'}', b','),
                 (IonType.LIST, b'[/**/', b'//\n]', b'//\n,'),
                 (IonType.SEXP, b'(//\n', b'/**/)', b'/**/'),  # TODO sexps without space delimiters are tested separately
                 (IonType.STRUCT, b'{/**/', b'//\n}', b'/**/,')):  # space after opening bracket for instant start_container event
        ion_type = info[0]
        param_generator = None
        for nested_param in nested_params:
            if param_generator is None:
                param_generator = nested_param(info[3])
            else:
                param_generator = nested_param(param_generator)
        params_list = list(param_generator)
        for param in params_list:
            @listify
            def add_field_names(event_pairs):
                for read_event, ion_event in event_pairs:
                    if read_event.type is ReadEventType.DATA:
                        field_name, expected_field_name = name_generator.next()
                        data = field_name + b':' + read_event.data
                        read_event = read_data_event(data)
                        ion_event = ion_event.derive_field_name(expected_field_name)
                    yield read_event, ion_event

            start = [
                (NEXT, END),
                (e_read(info[1]), e_start(ion_type)),
                (NEXT, INC)
            ]
            if ion_type is IonType.STRUCT:
                mid = add_field_names(param.event_pairs)
            else:
                mid = param.event_pairs
            end = [(e_read(info[2]), e_end(ion_type)), (NEXT, END)]
            desc = 'SINGLETON %s - %s' % (ion_type.name, param.desc)
            yield _P(
                desc=desc,
                event_pairs=start + mid + end,
            )


def _expect_event(expected_event, postpend, desc, text, events=()):
    """Generates event pairs for a stream that ends in an expected event (or exception), given the text and the output
    events preceding the expected event.
    """
    events += (expected_event,)
    outputs = events[1:]
    event_pairs = [(e_read(text + postpend), events[0])] + zip([NEXT]*len(outputs), outputs)
    return _P(
        desc='%s %s' % (desc, text),
        event_pairs=[(NEXT, END)] + event_pairs
    )


_ion_exception = partial(_expect_event, IonException, b' ', 'BAD')
_incomplete = partial(_expect_event, INC, b'', 'INC')

_BAD = (
    _ion_exception(b'+1'),
    _ion_exception(b'01'),
    _ion_exception(b'1__0'),
    _ion_exception(b'1_e1'),
    _ion_exception(b'1e_1'),
    _ion_exception(b'1e1_'),
    _ion_exception(b'1._0'),
    _ion_exception(b'1_.0'),
    _ion_exception(b'-_1'),
    _ion_exception(b'1_'),
    _ion_exception(b'0_x1'),
    _ion_exception(b'0b_1'),
    _ion_exception(b'null.strings'),
    _ion_exception(b'200T'),
    _ion_exception(b'2000-01'),
    _ion_exception(b'2007-02-23T20:14:33.Z'),
    _ion_exception(b'1a'),
    # _ion_exception(b'{{/**/}}'),  # TODO these are all lexed as blobs. Once base64 parsing is included, they will fail
    # _ion_exception(b'{{//\n}}'),
    # _ion_exception(b'{{/**/"abc"}}'),
    _ion_exception(b'{{"abc"//\n}}'),
    _ion_exception(b'{{\'\'\'abc\'\'\'//\n\'\'\'def\'\'\'}}'),
    _ion_exception(b'{{ abcd} }'),
    _ion_exception(b'{ {abcd}}', (e_start_struct(),)),
    _ion_exception(b'{foo:bar/**/baz:zar}', (e_start_struct(), e_symbol(value=b'bar', field_name=b'foo'))),
    _ion_exception(b'[abc 123]', (e_start_list(), e_symbol(value=b'abc'))),
    _ion_exception(b'{foo::bar:baz}', (e_start_struct(),)),
    _ion_exception(b'[abc, , 123]', (e_start_list(), e_symbol(value=b'abc'))),
    _ion_exception(b'{foo:bar, ,}', (e_start_struct(), e_symbol(value=b'bar', field_name=b'foo'),))
)

_INCOMPLETE = (
    _incomplete(b'{'),  # Might be a lob.
    _incomplete(b'{ ', (e_start_struct(),)),
    _incomplete(b'[', (e_start_list(),)),
    _incomplete(b'(', (e_start_sexp(),)),
    _incomplete(b'[[]', (e_start_list(), e_start_list(), e_end_list())),
    _incomplete(b'(()', (e_start_sexp(), e_start_sexp(), e_end_sexp())),
    _incomplete(b'{foo:{}', (e_start_struct(), e_start_struct(field_name=b'foo'), e_end_struct())),
    _incomplete(b'{foo:bar', (e_start_struct(),)),
    _incomplete(b'{foo:bar::', (e_start_struct(),)),
    _incomplete(b'{foo:bar,', (e_start_struct(), e_symbol(value=b'bar', field_name=b'foo'))),
    _incomplete(b'[[],', (e_start_list(), e_start_list(), e_end_list())),
    _incomplete(b'{foo:{},', (e_start_struct(), e_start_struct(field_name=b'foo'), e_end_struct())),
    _incomplete(b'foo'),  # Might be an annotation.
    _incomplete(b'\'foo\''),  # Might be an annotation.
    _incomplete(b'\'\'\'foo\'\'\'/**/'),  # Might be followed by another triple-quoted string.
    _incomplete(b'123'),  # Might have more digits.
    _incomplete(b'-'),
    _incomplete(b'1.2'),
    _incomplete(b'1.2e'),
    _incomplete(b'1.2e-'),
    _incomplete(b'1.2d'),
    _incomplete(b'1.2d3'),
    _incomplete(b'1_'),
    _incomplete(b'0b'),
    _incomplete(b'0x'),
    _incomplete(b'2000-01'),
    _incomplete(b'"abc'),
    _incomplete(b'false'),  # Might be a symbol with more characters.
    _incomplete(b'null.string'),  # Might be a symbol with more characters.
    _incomplete(b'/*'),
    _incomplete(b'//'),
    _incomplete(b'foo:'),
    _incomplete(b'foo::'),
    _incomplete(b'foo::bar'),
    _incomplete(b'foo//\n'),
    _incomplete(b'{foo', (e_start_struct(),)),
    _incomplete(b'{{'),
    _incomplete(b'{{"'),
    _incomplete(b'(foo-', (e_start_sexp(), e_symbol(value=b'foo'))),
    _incomplete(b'(-foo', (e_start_sexp(), e_symbol(value=b'-'))),
)


# TODO containers within containers
# TODO sexps without space delimiters

@parametrize(*chain(
    _BAD,
    _INCOMPLETE,
    _top_level_value_params(),  # all top-level values as individual data events, space-delimited
    _all_top_level_as_one_stream_params(),  # all top-level values as one data event, space-delimited
    _all_top_level_as_one_stream_params(b'/*foo*/'),  # all top-level values as one data event, block comment delimited
    _all_top_level_as_one_stream_params(b'//foo\n'),  # all top-level values as one data event, line comment delimited
    _annotate_params(_top_level_value_params()),  # all annotated top-level values, spaces postpended
    _annotate_params(_top_level_value_params(b'//foo\n/*bar*/')),  # all annotated top-level values, comments postpended
    _containerize_params((_scalar_params,)),  # all values, each as the only value within a container
    _containerize_params((_scalar_params, _annotate_params)),  # all values, each as the only value within a container
    _containerize_params((_all_scalars_in_one_container_params,)),  # all values within a single container
    _containerize_params((_all_scalars_in_one_container_params, _annotate_params)),  # all values annotated
))
def test_raw_reader(p):
    reader_scaffold(reader(), p.event_pairs)
