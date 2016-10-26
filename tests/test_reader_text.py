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
from amazon.ion.util import coroutine
from tests import listify, parametrize
from tests.event_aliases import *
from tests.reader_util import ReaderParameter, reader_scaffold

_b = bytearray
_P = ReaderParameter

_BAD = (
    (b'+1',),
    (b'01',),
    (b'1.23.4',),
    (b'1__0',),
    (b'1_e1',),
    (b'1e_1',),
    (b'1e1_',),
    (b'-infs',),
    (b'+infs',),
    (b'-in',),
    (b'1._0',),
    (b'1_.0',),
    (b'-_1',),
    (b'1_',),
    (b'0_x1',),
    (b'0b_1',),
    (b'1e0-1',),
    (b'1e0e-1',),
    (b'null.strings',),
    (b'200T',),
    (b'-2000T',),
    (b'-0001T',),
    (b'00-01T',),
    (b'2000-01',),
    (b'2007-02-23T20:14:33.Z',),
    (b'1a',),
    # (b'{{/**/}}',),  # TODO these are all lexed as blobs. Once base64 parsing is included, they will fail
    # (b'{{//\n}}',),
    # (b'{{/**/"abc"}}',),
    (b'{{"abc"//\n}}',),
    (b'{{\'\'\'abc\'\'\'//\n\'\'\'def\'\'\'}}',),
    (b'{{ abcd} }',),
    (b'{ {abcd}}', e_start_struct()),
    (b'{foo:bar/**/baz:zar}', e_start_struct(), e_symbol(value=b'bar', field_name=b'foo')),
    (b'[abc 123]', e_start_list(), e_symbol(value=b'abc')),
    (b'{foo::bar:baz}', e_start_struct()),
    (b'[abc, , 123]', e_start_list(), e_symbol(value=b'abc')),
    (b'{foo:bar, ,}', e_start_struct(), e_symbol(value=b'bar', field_name=b'foo')),
    (b'{true:123}', e_start_struct()),
    (b'{false:123}', e_start_struct()),
    (b'{+inf:123}', e_start_struct()),
    (b'{-inf:123}', e_start_struct()),
    (b'{nan:123}', e_start_struct()),
    (b'(1..)', e_start_sexp()),
    (b'(1.a)', e_start_sexp()),
    (b'(1.23.)', e_start_sexp()),
    (b'/ ',)
)

_INCOMPLETE = (
    (b'{',),  # Might be a lob.
    (b'{ ', e_start_struct()),
    (b'[', e_start_list()),
    (b'(', e_start_sexp()),
    (b'[[]', e_start_list(), e_start_list(), e_end_list()),
    (b'(()', e_start_sexp(), e_start_sexp(), e_end_sexp()),
    (b'{foo:{}', e_start_struct(), e_start_struct(field_name=b'foo'), e_end_struct()),
    (b'{foo:bar', e_start_struct(),),
    (b'{foo:bar::', e_start_struct(),),
    (b'{foo:bar,', e_start_struct(), e_symbol(value=b'bar', field_name=b'foo')),
    (b'[[],', e_start_list(), e_start_list(), e_end_list()),
    (b'{foo:{},', e_start_struct(), e_start_struct(field_name=b'foo'), e_end_struct()),
    (b'foo',),  # Might be an annotation.
    (b'\'foo\'',),  # Might be an annotation.
    (b'\'\'\'foo\'\'\'/**/',),  # Might be followed by another triple-quoted string.
    (b'123',),  # Might have more digits.
    (b'-',),
    (b'+',),
    (b'1.2',),
    (b'1.2e',),
    (b'1.2e-',),
    (b'+inf',),  # Might be followed by more characters, making it invalid at the top level.
    (b'-inf',),
    (b'nan',),
    (b'1.2d',),
    (b'1.2d3',),
    (b'1_',),
    (b'0b',),
    (b'0x',),
    (b'2000-01',),
    (b'"abc',),
    (b'false',),  # Might be a symbol with more characters.
    (b'true',),
    (b'null.string',),  # Might be a symbol with more characters.
    (b'/',),
    (b'/*',),
    (b'//',),
    (b'foo:',),
    (b'foo::',),
    (b'foo::bar',),
    (b'foo//\n',),
    (b'{foo', e_start_struct()),
    (b'{{',),
    (b'{{"',),
    (b'(foo-', e_start_sexp(), e_symbol(value=b'foo')),
    (b'(-foo', e_start_sexp(), e_symbol(value=b'-')),
)


def _good_sexp(*events):
    return (e_start_sexp(),) + events + (e_end_sexp(),)


def _good_struct(*events):
    return (e_start_struct(),) + events + (e_end_struct(),)


def _good_list(*events):
    return (e_start_list(),) + events + (e_end_list(),)


_GOOD = (
    (b'[]',) + _good_list(),
    (b'()',) + _good_sexp(),
    (b'{}',) + _good_struct(),
    (b'{/**/}',) + _good_struct(),
    (b'(/**/)',) + _good_sexp(),
    (b'[/**/]',) + _good_list(),
    (b'{//\n}',) + _good_struct(),
    (b'(//\n)',) + _good_sexp(),
    (b'[//\n]',) + _good_list(),
    (b'{/**///\n}',) + _good_struct(),
    (b'(/**///\n)',) + _good_sexp(),
    (b'[/**///\n]',) + _good_list(),
    (b'/*foo*///bar\n/*baz*/',),
    (b'\'\'::123 ', e_int(value=b'123', annotations=(b'',))),
    (b'{foo:zar::[], bar: (), baz:{}}',) + _good_struct(
        e_start_list(field_name=b'foo', annotations=(b'zar',)), e_end_list(),
        e_start_sexp(field_name=b'bar'), e_end_sexp(),
        e_start_struct(field_name=b'baz'), e_end_struct()
    ),
    (b'[[], zar::{}, ()]',) + _good_list(
        e_start_list(), e_end_list(),
        e_start_struct(annotations=(b'zar',)), e_end_struct(),
        e_start_sexp(), e_end_sexp(),
    ),
    (b'{\'\':bar,}',) + _good_struct(e_symbol(field_name=b'', value=b'bar')),
)


_UNSPACED_SEXPS = (
    (b'(foo //bar\n::baz)',) + _good_sexp(e_symbol(value=b'baz', annotations=(b'foo',))),
    (b'(foo/*bar*/ ::baz)',) + _good_sexp(e_symbol(value=b'baz', annotations=(b'foo',))),
    (b'(\'a b\' //\n::cd)',) + _good_sexp(e_symbol(value=b'cd', annotations=(b'a b',))),
    (b'(abc//baz\n-)',) + _good_sexp(e_symbol(b'abc'), e_symbol(b'-')),
    (b'(null-100/**/)',) + _good_sexp(e_null(), e_int(b'-100')),
    (b'(//\nnull//\n)',) + _good_sexp(e_null()),
    (b'(abc/*baz*/123)',) + _good_sexp(e_symbol(b'abc'), e_int(b'123')),
    (b'(abc/*baz*/-)',) + _good_sexp(e_symbol(b'abc'), e_symbol(b'-')),
    (b'(abc//baz\n123)',) + _good_sexp(e_symbol(b'abc'), e_int(b'123')),
    (b'(foo%+null-//\n)',) + _good_sexp(e_symbol(b'foo'), e_symbol(b'%+'), e_null(), e_symbol(b'-//')),  # Matches java.
    (b'(null-100)',) + _good_sexp(e_null(), e_int(b'-100')),
    (b'(null.string.b)',) + _good_sexp(e_string(None), e_symbol(b'.'), e_symbol(b'b')),
    (b'(-100)',) + _good_sexp(e_int(b'-100')),
    (b'(-1.23 .)',) + _good_sexp(e_decimal(b'-1.23'), e_symbol(b'.')),
    (b'(1.)',) + _good_sexp(e_decimal(b'1.')),
    (b'(1. .1)',) + _good_sexp(e_decimal(b'1.'), e_symbol(b'.'), e_int(b'1')),
    (b'(nul)',) + _good_sexp(e_symbol(b'nul')),
    (b'(foo::%-bar)',) + _good_sexp(e_symbol(value=b'%-', annotations=(b'foo',)), e_symbol(b'bar')),
    (b'(true.False+)',) + _good_sexp(e_bool(True), e_symbol(b'.'), e_symbol(b'False'), e_symbol(b'+')),
    (b'(false)',) + _good_sexp(e_bool(False)),
    (b'(-inf)',) + _good_sexp(e_float(b'-inf')),
    (b'(+inf)',) + _good_sexp(e_float(b'+inf')),
    (b'(nan)',) + _good_sexp(e_float(b'nan')),
    (b'(-inf+inf)',) + _good_sexp(e_float(b'-inf'), e_float(b'+inf')),
    # TODO the inf tests do not match ion-java's behavior. They should be reconciled. I believe this is more correct.
    (b'(- -inf-inf-in-infs-)',) + _good_sexp(
        e_symbol(b'-'), e_float(b'-inf'), e_float(b'-inf'), e_symbol(b'-'),
        e_symbol(b'in'), e_symbol(b'-'), e_symbol(b'infs'), e_symbol(b'-')
    ),
    (b'(+ +inf+inf+in+infs+)',) + _good_sexp(
        e_symbol(b'+'), e_float(b'+inf'), e_float(b'+inf'), e_symbol(b'+'),
        e_symbol(b'in'), e_symbol(b'+'), e_symbol(b'infs'), e_symbol(b'+')
    ),
    (b'(nan-nan+nan)',) + _good_sexp(
        e_float(b'nan'), e_symbol(b'-'), e_float(b'nan'), e_symbol(b'+'),
        e_float(b'nan')
    ),
    (b'(nans-inf+na-)',) + _good_sexp(
        e_symbol(b'nans'), e_float(b'-inf'), e_symbol(b'+'),
        e_symbol(b'na'), e_symbol(b'-')
    ),
    (b'({}()zar::[])',) + _good_sexp(
        e_start_struct(), e_end_struct(),
        e_start_sexp(), e_end_sexp(),
        e_start_list(annotations=(b'zar',)), e_end_list()
    ),
)

_GOOD_SCALARS = (
    (b'null', e_null()),

    (b'false', e_bool(False)),
    (b'true', e_bool(True)),
    (b'null.bool', e_bool()),

    (b'null.int', e_int()),
    (b'0', e_int(_b(b'0'))),
    (b'1_2_3', e_int(_b(b'123'))),
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
    (b'0.0E1', e_float(_b(b'0.0E1'))),
    (b'-inf', e_float(_b(b'-inf'))),
    (b'+inf', e_float(_b(b'+inf'))),
    (b'nan', e_float(_b(b'nan'))),

    (b'null.decimal', e_decimal()),
    (b'0.0', e_decimal(_b(b'0.0'))),
    (b'0.', e_decimal(_b(b'0.'))),
    (b'-0.0', e_decimal(_b(b'-0.0'))),
    (b'0d-1000', e_decimal(_b(b'0d-1000'))),
    (b'0d1000', e_decimal(_b(b'0d1000'))),
    (b'1d1', e_decimal(_b(b'1d1'))),
    (b'1D1', e_decimal(_b(b'1D1'))),
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
    (b'nul', e_symbol(_b(b'nul'))),  # See the logic in the event generators that forces these to emit an event.
    (b'$foo', e_symbol(_b(b'$foo'))),
    (b'\'a b\'', e_symbol(_b(b'a b'))),
    (b'\'\'', e_symbol(_b(b''))),

    (b'null.string', e_string()),
    (b'" "', e_string(_b(b' '))),
    (b'\'\'\'foo\'\'\' \'\'\'\'\'\' \'\'\'""\'\'\'', e_string(_b(b'foo""'))),
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
)


def _scalar_event_pairs(data, events, delimiter):
    # TODO duplicated in test_reader_binary -- consolidate in reader_util
    first = True
    space_delimited = not (b',' in delimiter)
    for event in events:
        input_event = NEXT
        if first:
            input_event = e_read(data + delimiter)
            if space_delimited and event.value is not None \
                and ((event.ion_type is IonType.SYMBOL) or
                     (event.ion_type is IonType.STRING and b'"' != data[0])):  # triple-quoted strings
                # Because annotations and field names are symbols, a space delimiter after a symbol isn't enough to
                # generate a symbol event. Similarly, triple-quoted strings may be followed by another triple-quoted
                # string if only delimited by whitespace or comments. To address this issue, these types
                # are delimited in these tests by another value - in this case, int 0 (but it could be anything).
                yield input_event, INC
                yield e_read(b'0' + delimiter), event
                input_event, event = (NEXT, e_int(b'0'))
            first = False
        yield input_event, event


def _value_iter(event_func, values, delimiter):
    # TODO duplicated in test_reader_binary -- consolidate in reader_util
    for seq in values:
        data = seq[0]
        event_pairs = list(event_func(data, seq[1:], delimiter))
        yield data, event_pairs


_scalar_iter = partial(_value_iter, _scalar_event_pairs, _GOOD_SCALARS)


@coroutine
def _scalar_params():
    while True:
        delimiter = yield
        for data, event_pairs in _scalar_iter(delimiter):
            yield _P(
                desc=data,
                event_pairs=event_pairs + [(NEXT, INC)]
            )


def _top_level_value_params(delimiter=b' ', is_delegate=False):
    # TODO duplicated in test_reader_binary -- consolidate in reader_util
    """Converts the top-level tuple list into parameters with appropriate ``NEXT`` inputs.

    The expectation is starting from an end of stream top-level context.
    """
    for data, event_pairs in _scalar_iter(delimiter):
        _, first = event_pairs[0]
        if first.event_type is IonEventType.INCOMPLETE:  # Happens with space-delimited symbol values.
            _, first = event_pairs[1]
        yield _P(
            desc='TL %s - %s - %r' % \
                 (first.event_type.name, first.ion_type.name, data),
            event_pairs=[(NEXT, END)] + event_pairs + [(NEXT, END)],
        )
    if is_delegate:
        yield


@coroutine
def _all_scalars_in_one_container_params():
    # TODO duplicated in test_reader_binary -- consolidate in reader_util
    while True:
        delimiter = yield

        @listify
        def generate_event_pairs():
            for data, event_pairs in _scalar_iter(delimiter):
                pairs = ((i, o) for i, o in event_pairs)
                while True:
                    try:
                        input_event, output_event = pairs.next()
                        yield input_event, output_event
                        if output_event is INC:
                            # This is a symbol value.
                            yield pairs.next()  # Input: a scalar. Output: the symbol value's event.
                            yield pairs.next()  # Input: NEXT. Output: the previous scalar's event.
                        yield (NEXT, INC)
                    except StopIteration:
                        break

        yield _P(
            desc='ALL',
            event_pairs=generate_event_pairs()
        )


def _all_top_level_as_one_stream_params(delimiter=b' '):
    # TODO duplicated in test_reader_binary -- consolidate in reader_util
    @listify
    def generate_event_pairs():
        yield (NEXT, END)
        for data, event_pairs in _scalar_iter(delimiter):
            for event_pair in event_pairs:
                yield event_pair
            yield (NEXT, END)

    yield _P(
        desc='TOP LEVEL ALL',
        event_pairs=generate_event_pairs()
    )


def _collect_params(param_generator, delimiter):
    """Collect all output of the given coroutine into a single list."""
    params = []
    while True:
        param = param_generator.send(delimiter)
        if param is None:
            return params
        params.append(param)


_TEST_SYMBOLS = (
    (
        b'foo',
        b'$foo',
        b'\'a b\'',
        b'foo ',
        b'\'a b\' ',
        b'foo/*bar*/',
        b'\'a b\' //bar\n',
        b'\'\'',
    ),
    (
        b'foo',
        b'$foo',
        b'a b',
        b'foo',
        b'a b',
        b'foo',
        b'a b',
        b'',
    )
)


def _generate_annotations():
    assert len(_TEST_SYMBOLS[0]) == len(_TEST_SYMBOLS[1])
    i = 1
    num_symbols = len(_TEST_SYMBOLS[0])
    while True:
        yield _TEST_SYMBOLS[0][0:i], _TEST_SYMBOLS[1][0:i]
        i += 1
        if i == num_symbols:
            i = 0


_annotations_generator = _generate_annotations()


@coroutine
def _annotate_params(params, is_delegate=False):
    """Adds annotation wrappers for a given iterator of parameters,

    The requirement is that the given parameters completely encapsulate a single value.
    """

    while True:
        delimiter = yield
        params_list = _collect_params(params, delimiter)
        test_annotations, expected_annotations = _annotations_generator.next()
        for param in params_list:
            @listify
            def annotated():
                pairs = ((i, o) for i, o in param.event_pairs)
                while True:
                    try:
                        input_event, output_event = pairs.next()
                        if input_event.type is ReadEventType.DATA:
                            data = b''
                            for test_annotation in test_annotations:
                                data += test_annotation + b'::'
                            data += input_event.data
                            input_event = read_data_event(data)
                            if output_event is INC:
                                yield input_event, output_event
                                input_event, output_event = pairs.next()
                            output_event = output_event.derive_annotations(expected_annotations)
                        yield input_event, output_event
                    except StopIteration:
                        break

            yield _P(
                desc='ANN %r on %s' % (expected_annotations, param.desc),
                event_pairs=annotated(),
            )
        if not is_delegate:
            break


def _generate_field_name():
    assert len(_TEST_SYMBOLS[0]) == len(_TEST_SYMBOLS[1])
    i = 0
    num_symbols = len(_TEST_SYMBOLS[0])
    while True:
        yield _TEST_SYMBOLS[0][i], _TEST_SYMBOLS[1][i]
        i += 1
        if i == num_symbols:
            i = 0


_field_name_generator = _generate_field_name()


@coroutine
def _containerize_params(param_generator, with_skip=True, is_delegate=False, top_level=True):
    """Adds container wrappers for a given iteration of parameters.

    The requirement is that each parameter is a self-contained single value.
    """
    # TODO skipping
    while True:
        yield
        for info in ((IonType.LIST, b'[', b']', b','),
                     (IonType.SEXP, b'(', b')', b' '),  # TODO sexps without space delimiters are tested separately
                     (IonType.STRUCT, b'{ ', b'}', b','),
                     (IonType.LIST, b'[/**/', b'//\n]', b'//\n,'),
                     (IonType.SEXP, b'(//\n', b'/**/)', b'/**/'),  # TODO sexps without space delimiters are tested separately
                     (IonType.STRUCT, b'{/**/', b'//\n}', b'/**/,')):  # space after opening bracket for instant start_container event
            ion_type = info[0]
            params = _collect_params(param_generator, info[3])
            for param in params:
                @listify
                def add_field_names(event_pairs):
                    container = False
                    first = True
                    for read_event, ion_event in event_pairs:
                        if not container and read_event.type is ReadEventType.DATA:
                            field_name, expected_field_name = _field_name_generator.next()
                            data = field_name + b':' + read_event.data
                            read_event = read_data_event(data)
                            ion_event = ion_event.derive_field_name(expected_field_name)
                        if first and ion_event.event_type is IonEventType.CONTAINER_START:
                            # For containers within a struct--only the CONTAINER_START event gets adorned with a
                            # field name
                            container = True
                        first = False
                        yield read_event, ion_event
                start = []
                end = [(e_read(info[2]), e_end(ion_type))]
                if top_level:
                    start = [(NEXT, END)]
                    end += [(NEXT, END)]
                else:
                    end += [(NEXT, INC)]
                start += [
                    (e_read(info[1]), e_start(ion_type)),
                    (NEXT, INC)
                ]
                if ion_type is IonType.STRUCT:
                    mid = add_field_names(param.event_pairs)
                else:
                    mid = param.event_pairs
                desc = 'SINGLETON %s - %s' % (ion_type.name, param.desc)
                yield _P(
                    desc=desc,
                    event_pairs=start + mid + end,
                )
        if not is_delegate:
            break


def _expect_event(expected_event, data, events, delimiter):
    """Generates event pairs for a stream that ends in an expected event (or exception), given the text and the output
    events preceding the expected event.
    """
    events += (expected_event,)
    outputs = events[1:]
    event_pairs = [(e_read(data + delimiter), events[0])] + zip([NEXT] * len(outputs), outputs)
    return event_pairs


@coroutine
def _basic_params(event_func, desc, delimiter, data_event_pairs, is_delegate=False, top_level=True):
    while True:
        yield
        for data, events in _value_iter(event_func, data_event_pairs, delimiter):
            event_pairs = []
            if top_level:
                event_pairs += [(NEXT, END)]
            event_pairs += events
            yield _P(
                desc='%s %s' % (desc, data),
                event_pairs=event_pairs
            )
        if not is_delegate:
            break


_ion_exception = partial(_expect_event, IonException)
_bad_params = partial(_basic_params, _ion_exception, 'BAD', b' ')
_incomplete = partial(_expect_event, INC)
_incomplete_params = partial(_basic_params, _incomplete, 'INC', b'')
_end = partial(_expect_event, END)
_good_params = partial(_basic_params, _end, 'GOOD', b'')


@parametrize(*chain(
    _good_params(_GOOD),
    _bad_params(_BAD),
    _incomplete_params(_INCOMPLETE),
    _good_params(_UNSPACED_SEXPS),
    _top_level_value_params(),  # all top-level values as individual data events, space-delimited
    _all_top_level_as_one_stream_params(),  # all top-level values as one data event, space-delimited
    _all_top_level_as_one_stream_params(b'/*foo*/'),  # all top-level values as one data event, block comment delimited
    _all_top_level_as_one_stream_params(b'//foo\n'),  # all top-level values as one data event, line comment delimited
    _annotate_params(_top_level_value_params(is_delegate=True)),  # all annotated top-level values, spaces postpended
    _annotate_params(_top_level_value_params(b'//foo\n/*bar*/', is_delegate=True)),  # all annotated top-level values, comments postpended
    _annotate_params(_good_params(_UNSPACED_SEXPS, is_delegate=True)),
    _containerize_params(_scalar_params()),  # all values, each as the only value within a container
    _containerize_params(_containerize_params(_scalar_params(), is_delegate=True, top_level=False)),
    _containerize_params(_annotate_params(_scalar_params(), is_delegate=True)),  # all values, each as the only value within a container
    _containerize_params(_all_scalars_in_one_container_params()),  # all values within a single container
    _containerize_params(_annotate_params(_all_scalars_in_one_container_params(), is_delegate=True)),  # annotated containers
    _containerize_params(_annotate_params(_incomplete_params(_UNSPACED_SEXPS, is_delegate=True, top_level=False), is_delegate=True)),
))
def test_raw_reader(p):
    reader_scaffold(reader(), p.event_pairs)
