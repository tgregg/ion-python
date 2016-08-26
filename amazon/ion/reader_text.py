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

from collections import defaultdict

from amazon.ion.core import Transition, ION_STREAM_INCOMPLETE_EVENT, ION_STREAM_END_EVENT, IonType, IonEvent, \
    IonEventType
from amazon.ion.exceptions import IonException
from amazon.ion.reader import BufferQueue, reader_trampoline
from amazon.ion.util import record, coroutine, Enum


#def character_table(table):
#    return defaultdict(lambda k: _ion_exception('Illegal character %s found.' % (k,)), table)

def get(table, c):
    if c not in table:
        raise IonException('Illegal character %s' % (chr(c),))
    return table[c]


@coroutine
def number_negative_start_handler(c, ctx):
    assert c == ord('-')
    assert len(ctx.value) == 0
    ctx = ctx.derive_ion_type(IonType.INT)
    ctx.value.append(c)
    c, _ = yield
    yield ctx.immediate_transition(get(_NEGATIVE_TABLE, c)(c, ctx))


@coroutine
def number_zero_start_handler(c, ctx):
    assert c == ord('0')
    assert len(ctx.value) == 0 or (len(ctx.value) == 1 and ctx.value[0] == ord('-'))
    ctx = ctx.derive_ion_type(IonType.INT)
    ctx.value.append(c)
    c, _ = yield
    yield ctx.immediate_transition(get(_ZERO_START_TABLE, c)(c, ctx))


@coroutine
def number_or_timestamp_handler(c, ctx):
    assert c in _DIGITS
    ctx = ctx.derive_ion_type(IonType.INT)  # If this is the last digit read, this value is an Int.
    val = ctx.value
    val.append(c)
    c, self = yield
    trans = Transition(None, self)
    while True:
        if c in _NUMBER_END_SEQUENCE[0]:
            trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
        else:
            if c not in _DIGITS:
                trans = ctx.immediate_transition(get(_NUMBER_OR_TIMESTAMP_TABLE, c)(c, ctx))
            else:
                val.append(c)
        c, _ = yield trans


_UNDERSCORE = ord('_')


@coroutine
def number_handler(c, ctx):
    val = ctx.value
    if c != _UNDERSCORE:
        val.append(c)
    prev = c
    c, self = yield
    trans = Transition(None, self)
    while True:
        if c in _NUMBER_END_SEQUENCE[0]:
            if prev == _UNDERSCORE:
                raise IonException('%s at end of int' % (chr(prev),))
            trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
        else:
            if c == _UNDERSCORE:
                if prev == _UNDERSCORE:
                    raise IonException('Underscore after %s' % (chr(prev),))
            else:
                if c not in _DIGITS:
                    if prev == _UNDERSCORE:
                        raise IonException('Underscore before %s' % (chr(c),))
                    trans = ctx.immediate_transition(_NUMBER_TABLE[c](c, ctx))
                else:
                    val.append(c)
        prev = c
        c, _ = yield trans


@coroutine
def real_number_handler(c, ctx):
    assert c == ord('.')
    ctx = ctx.derive_ion_type(IonType.DECIMAL)  # If this is the last character read, this value is a decimal
    val = ctx.value
    val.append(c)
    prev = c
    c, self = yield
    if c == _UNDERSCORE:
        raise IonException('Underscore after decimal point')
    trans = Transition(None, self)
    while True:
        if c in _NUMBER_END_SEQUENCE[0]:
            if prev == _UNDERSCORE:
                raise IonException('%s at end of decimal' % (chr(prev),))
            trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
        else:
            if c == _UNDERSCORE:
                if prev == _UNDERSCORE:
                    raise IonException('Underscore after %s' % (chr(prev),))
            else:
                if c not in _DIGITS:
                    if prev == _UNDERSCORE:
                        raise IonException('Underscore before %s' % (chr(c),))
                    trans = ctx.immediate_transition(get(_REAL_NUMBER_TABLE, c)(c, ctx))
                else:
                    val.append(c)
        prev = c
        c, _ = yield trans


@coroutine
def decimal_handler(c, ctx):
    assert c == ord('d') or c == ord('D')
    # assert ctx.ion_type == IonType.DECIMAL  # Not in the case of leading zero, e.g. 0d0
    ctx = ctx.derive_ion_type(IonType.DECIMAL)  # If this is the last character read, this value is a decimal
    val = ctx.value
    val.append(c)
    prev = c
    c, self = yield
    if c == _UNDERSCORE:
        raise IonException('Underscore after exponent')
    trans = Transition(None, self)
    while True:
        if c in _NUMBER_END_SEQUENCE[0]:
            if prev == _UNDERSCORE or prev == ord('d') or c == ord('D'):
                raise IonException('%s at end of decimal' % (chr(prev),))
            trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
        else:
            if c == _UNDERSCORE:
                if prev == _UNDERSCORE or prev == ord('d') or c == ord('D'):
                    raise IonException('Underscore after %s' % (chr(prev),))
            else:
                if c not in _DIGITS:
                    trans = ctx.immediate_transition(_DECIMAL_TABLE[c](c, ctx))
                else:
                    val.append(c)
        prev = c
        c, _ = yield trans


@coroutine
def float_handler(c, ctx):
    assert c == ord('e') or c == ord('E')
    ctx = ctx.derive_ion_type(IonType.FLOAT)
    val = ctx.value
    val.append(c)
    prev = c
    c, self = yield
    if c == _UNDERSCORE:
        raise IonException('Underscore after exponent')
    trans = Transition(None, self)
    while True:
        if c in _NUMBER_END_SEQUENCE[0]:
            if prev == _UNDERSCORE or prev == ord('d') or c == ord('E'):
                raise IonException('%s at end of decimal' % (chr(prev),))
            trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
        else:
            if c == _UNDERSCORE:
                if prev == _UNDERSCORE or prev == ord('e') or c == ord('E'):
                    raise IonException('Underscore after %s' % (chr(prev),))
                val.append(c)
            else:
                if c not in _DIGITS:
                    trans = ctx.immediate_transition(_FLOAT_TABLE[c](c, ctx))
                else:
                    val.append(c)
        prev = c
        c, _ = yield trans


@coroutine
def binary_int_handler(c, ctx):
    assert c == ord('b') or c == ord('B')
    assert (len(ctx.value) == 1 and ctx.value[0] == ord('0')) \
        or (len(ctx.value) == 2 and ctx.value[0] == ord('-') and ctx.value[1] == ord('0'))
    assert ctx.ion_type == IonType.INT
    val = ctx.value
    val.append(c)
    prev = c
    c, self = yield
    if c == _UNDERSCORE:
        raise IonException('Underscore after radix')
    trans = Transition(None, self)
    while True:
        if c in _NUMBER_END_SEQUENCE[0]:
            if prev == _UNDERSCORE or prev == ord('b') or prev == ord('B'):
                raise IonException('%s at end of binary int' % (chr(prev),))
            trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
        else:
            if c == _UNDERSCORE:
                if prev == _UNDERSCORE or prev == ord('b') or prev == ord('B'):
                    raise IonException('Underscore after %s' % (chr(prev),))
            else:
                if c not in _BINARY_DIGITS:
                    trans = ctx.immediate_transition(get(_BINARY_INT_TABLE, c)(c, ctx))
                else:
                    val.append(c)
        prev = c
        c, _ = yield trans


@coroutine
def hex_int_handler(c, ctx):
    assert c == ord('x') or c == ord('X')
    assert (len(ctx.value) == 1 and ctx.value[0] == ord('0')) \
        or (len(ctx.value) == 2 and ctx.value[0] == ord('-') and ctx.value[1] == ord('0'))
    assert ctx.ion_type == IonType.INT
    val = ctx.value
    val.append(c)
    prev = c
    c, self = yield
    if c == _UNDERSCORE:
        raise IonException('Underscore after radix')
    trans = Transition(None, self)
    while True:
        if c in _NUMBER_END_SEQUENCE[0]:
            if prev == _UNDERSCORE or prev == ord('x') or prev == ord('X'):
                raise IonException('%s at end of hex int' % (chr(prev),))
            trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
        else:
            if c == _UNDERSCORE:
                if prev == _UNDERSCORE or prev == ord('x') or prev == ord('X'):
                    raise IonException('Underscore after %s' % (chr(prev),))
            else:
                if c not in _HEX_DIGITS:
                    trans = ctx.immediate_transition(get(_HEX_INT_TABLE, c)(c, ctx))
                else:
                    val.append(c)
        prev = c
        c, _ = yield trans


_TIMESTAMP_DELIMITERS = (
    ord('-'),
    ord(':'),
    ord('+'),
    ord('.')
)

_TIMESTAMP_OFFSET_INDICATORS = (
    ord('Z'),
    ord('+'),
    ord('-'),
)


@coroutine
def timestamp_zero_start_handler(c, ctx):
    assert len(ctx.value) == 1
    assert ctx.value[0] == ord('0')
    ctx = ctx.derive_ion_type(IonType.TIMESTAMP)
    val = ctx.value
    if val[0] == ord('-'):
        raise IonException('Negative not allowed in timestamp')
    val.append(c)
    c, self = yield
    trans = Transition(None, self)
    while True:
        if c == ord('-') or c == ord('T'):
            trans = ctx.immediate_transition(timestamp_handler(c, ctx))
        elif c in _DIGITS:
            val.append(c)
        else:
            raise IonException('Illegal character %s in timestamp' % (chr(c),))
        c, _ = yield trans


@coroutine
def timestamp_handler(c, ctx):
    class State(Enum):
        YEAR = 0
        MONTH = 1
        DAY = 2
        HOUR = 3
        MINUTE = 4
        SECOND = 5
        FRACTIONAL = 6
        OFF_HOUR = 7
        OFF_MINUTE = 8

    assert c == ord('T') or c == ord('-')
    if len(ctx.value) != 4:  # or should this be left to the parser?
        raise IonException('Timestamp year is %d digits; expected 4' % (len(ctx.value),))
    ctx = ctx.derive_ion_type(IonType.TIMESTAMP)
    val = ctx.value
    val.append(c)
    prev = c
    c, self = yield
    trans = Transition(None, self)
    state = State.YEAR
    nxt = _DIGITS
    if prev == ord('T'):
        nxt += _NUMBER_END_SEQUENCE[0]
    while True:
        if c not in nxt:
            raise IonException("Illegal character %s in timestamp; expected %r in state %r "
                               % (chr(c), [chr(x) for x in nxt], state))
        if c in _NUMBER_END_SEQUENCE[0]:
            trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
        else:
            if c == ord('Z'):
                nxt = _NUMBER_END_SEQUENCE[0]
            elif c == ord('T'):
                nxt = _NUMBER_END_SEQUENCE[0] + _DIGITS
            elif c in _TIMESTAMP_DELIMITERS:
                nxt = _DIGITS
            elif c in _DIGITS:
                if prev == ord('+') or (state > State.MONTH and prev == ord('-')):
                    state = State.OFF_HOUR
                elif prev in (_TIMESTAMP_DELIMITERS + (ord('T'),)):
                    state = State[state + 1]
                elif prev in _DIGITS:
                    if state == State.MONTH:
                        nxt = (ord('-'), ord('T'))
                    elif state == State.DAY:
                        nxt = (ord('T'),) + _NUMBER_END_SEQUENCE[0]
                    elif state == State.HOUR:
                        nxt = (ord(':'),)
                    elif state == State.MINUTE:
                        nxt = _TIMESTAMP_OFFSET_INDICATORS + (ord(':'),)
                    elif state == State.SECOND:
                        nxt = _TIMESTAMP_OFFSET_INDICATORS + (ord('.'),)
                    elif state == State.FRACTIONAL:
                        nxt = _DIGITS + _TIMESTAMP_OFFSET_INDICATORS + (ord('.'),)
                    elif state == State.OFF_HOUR:
                        nxt = (ord(':'),) + _NUMBER_END_SEQUENCE[0]
                    elif state == State.OFF_MINUTE:
                        nxt = _NUMBER_END_SEQUENCE[0]
                    else:
                        raise ValueError("Unknown timestamp state %r" % (state,))
                else:
                    raise IonException("Digit following %s in timestamp" % (chr(prev),))
            val.append(c)
        prev = c
        c, _ = yield trans


@coroutine
def string_handler(c, ctx):
    assert c == ord('"')
    ctx = ctx.derive_ion_type(IonType.STRING)
    val = ctx.value
    prev = c
    c, self = yield
    trans = Transition(None, self)
    done = False
    while not done:
        # TODO error on disallowed escape sequences
        if c == ord('"') and prev != ord('\\'):
            done = True
        else:
            # TODO should a backslash be appended?
            val.append(c)
        prev = c
        c, _ = yield trans
    yield ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)


@coroutine
def comment_handler(c, whence):
    assert c == ord('/')
    c, self = yield
    if c == ord('/'):
        block_comment = False
    elif c == ord('*'):
        block_comment = True
    else:
        raise IonException("Illegal character sequence '/%s'" % (chr(c),))
    done = False
    prev = None
    trans = Transition(None, self)
    while not done:
        c, _ = yield trans
        if block_comment:
            if prev == ord('*') and c == ord('/'):
                done = True
            prev = c
        else:
            if c == ord('\n'):
                done = True
    yield Transition(None, whence)


@coroutine
def triple_quote_string_handler(c, ctx):
    # TODO handle the case where the stream ends on a triple-quoted string. Emit an event or not?
    assert c == ord('\'')
    ctx = ctx.derive_ion_type(IonType.STRING)
    quotes = 0
    in_data = True
    val = ctx.value
    prev = c
    c, self = yield
    here = Transition(None, self)
    while True:
        trans = here
        # TODO error on disallowed escape sequences
        if c == ord('\'') and prev != ord('\\'):
            quotes += 1
            if quotes == 3:
                in_data = not in_data
                quotes = 0
        else:
            if in_data:
                # Any quotes found in the meantime are part of the data
                for i in range(quotes):
                    val.append(ord('\''))
                quotes = 0
                # TODO should a backslash be appended - why is Java inconsistent between double- and triple-quoted?
                if c != ord('\\'):
                    val.append(c)
            else:
                if quotes > 0:
                    ctx.queue.unread(quotes + 1)  # un-read the skipped quotes AND c, which will be consumed again later
                    trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
                elif c not in _WHITESPACE:
                    if c == ord('/'):
                        trans = ctx.immediate_transition(comment_handler(c, self))
                    else:
                        trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
        prev = c
        c, _ = yield trans


@coroutine
def string_or_symbol_handler(c, ctx):
    assert c == ord('\'')
    ctx = ctx.derive_ion_type(IonType.SYMBOL)
    c, self = yield
    if c == ord('\''):
        yield ctx.immediate_transition(two_single_quotes_handler(c, ctx))
    else:
        yield ctx.immediate_transition(quoted_symbol_handler(c, ctx))


@coroutine
def two_single_quotes_handler(c, ctx):
    assert c == ord('\'')
    c, self = yield
    if c == ord('\''):
        yield ctx.immediate_transition(triple_quote_string_handler(c, ctx))
    else:
        # This is the empty symbol
        assert ctx.ion_type == IonType.SYMBOL
        assert len(ctx.value) == 0
        yield ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)


@coroutine
def null_handler(c, ctx):
    raise IonException('Not yet implemented')


@coroutine
def symbol_or_null_handler(c, ctx):
    raise IonException('Not yet implemented')


@coroutine
def quoted_symbol_handler(c, ctx):
    assert c != ord('\'')
    val = ctx.value
    val.append(c)
    prev = c
    c, self = yield
    trans = Transition(None, self)
    done = False
    while not done:
        # TODO error on disallowed escape sequences
        if c == ord('\'') and prev != ord('\\'):
            done = True
        else:
            # TODO should a backslash be appended?
            val.append(c)
        prev = c
        c, _ = yield trans
    yield ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)

@coroutine
def symbol_handler(c, ctx):
    raise IonException('Not yet implemented')


@coroutine
def struct_or_lob_handler(c, ctx):
    assert c == ord('{')
    c, self = yield
    yield ctx.immediate_transition(_STRUCT_OR_LOB_TABLE[c](c, ctx))


@coroutine
def lob_handler(c, ctx):
    raise IonException('Not yet implemented')


@coroutine
def struct_handler(c, ctx):
    raise IonException('Not yet implemented')


@coroutine
def list_handler(c, ctx):
    raise IonException('Not yet implemented')


@coroutine
def sexp_handler(c, ctx):
    raise IonException('Not yet implemented')


_DIGITS = (
    ord('0'),
    ord('1'),
    ord('2'),
    ord('3'),
    ord('4'),
    ord('5'),
    ord('6'),
    ord('7'),
    ord('8'),
    ord('9')
)

_BINARY_DIGITS = (
    ord('0'),
    ord('1')
)

_HEX_DIGITS = (
    ord('0'),
    ord('1'),
    ord('2'),
    ord('3'),
    ord('4'),
    ord('5'),
    ord('6'),
    ord('7'),
    ord('8'),
    ord('9'),
    ord('a'),
    ord('b'),
    ord('c'),
    ord('d'),
    ord('e'),
    ord('f')
)

_DECIMAL_TABLE = {}
_FLOAT_TABLE = {}
_BINARY_INT_TABLE = {}
_HEX_INT_TABLE = {}

_REAL_NUMBER_TABLE = {
    ord('d'): decimal_handler,
    ord('e'): float_handler,
    ord('D'): decimal_handler,
    ord('E'): float_handler,
}

_ZERO_START_TABLE = {
    ord('0'): timestamp_zero_start_handler,  # TODO pythonic way to crunch these in?
    ord('1'): timestamp_zero_start_handler,
    ord('2'): timestamp_zero_start_handler,
    ord('3'): timestamp_zero_start_handler,
    ord('4'): timestamp_zero_start_handler,
    ord('5'): timestamp_zero_start_handler,
    ord('6'): timestamp_zero_start_handler,
    ord('7'): timestamp_zero_start_handler,
    ord('8'): timestamp_zero_start_handler,
    ord('9'): timestamp_zero_start_handler,
    ord('b'): binary_int_handler,
    ord('x'): hex_int_handler,
    ord('B'): binary_int_handler,
    ord('X'): hex_int_handler,
    ord('.'): real_number_handler,
    # ord('_'): number_handler,  # TODO leading zero not allowed for numbers -- is this the best way?
    ord('d'): decimal_handler,
    ord('e'): float_handler,
    ord('D'): decimal_handler,
    ord('E'): float_handler
}

_NUMBER_OR_TIMESTAMP_TABLE = {
    ord('-'): timestamp_handler,
    ord('T'): timestamp_handler,
    ord('.'): real_number_handler,
    ord('_'): number_handler,
    ord('d'): decimal_handler,
    ord('e'): float_handler,
    ord('D'): decimal_handler,
    ord('E'): float_handler,
}

_NUMBER_TABLE = {
    ord('.'): real_number_handler,
    ord('d'): decimal_handler,
    ord('e'): float_handler,
    ord('D'): decimal_handler,
    ord('E'): float_handler,
}

_NEGATIVE_TABLE = {
    ord('0'): number_zero_start_handler,
    ord('1'): number_handler,
    ord('2'): number_handler,
    ord('3'): number_handler,
    ord('4'): number_handler,
    ord('5'): number_handler,
    ord('6'): number_handler,
    ord('7'): number_handler,
    ord('8'): number_handler,
    ord('9'): number_handler,
}

_N_TABLE = {
    ord('u'): symbol_or_null_handler,
    # TODO all other symbol chars go to symbol_handler
}

_NU_TABLE = {
    ord('l'): symbol_or_null_handler,
    # TODO all other symbol chars go to symbol_handler
}

_NUL_TABLE = _NU_TABLE

_NULL_TABLE = {
    ord('.'): null_handler,
    # TODO delimiter goes to null handler
    # TODO all other symbol chars go to symbol handler
}

_STRUCT_OR_LOB_TABLE = defaultdict(
    lambda: struct_handler,
    {
        ord('{'): lob_handler
    }
)

_START_TABLE = {
    ord('n'): symbol_or_null_handler,
    ord('-'): number_negative_start_handler,
    ord('0'): number_zero_start_handler,
    ord('1'): number_or_timestamp_handler,
    ord('2'): number_or_timestamp_handler,
    ord('3'): number_or_timestamp_handler,
    ord('4'): number_or_timestamp_handler,
    ord('5'): number_or_timestamp_handler,
    ord('6'): number_or_timestamp_handler,
    ord('7'): number_or_timestamp_handler,
    ord('8'): number_or_timestamp_handler,
    ord('9'): number_or_timestamp_handler,
    ord('{'): struct_or_lob_handler,
    ord('('): sexp_handler,
    ord('['): list_handler,
    ord('\''): string_or_symbol_handler,
    ord('\"'): string_handler,
}


class _HandlerContext(record(
    'end_sequence', 'queue', 'field_name', 'annotations', 'depth', 'whence', 'value', 'ion_type'
)):
    """TODO
    """

    def event_transition(self, event_cls, event_type,
                         ion_type=None, value=None, annotations=None, depth=None, whence=None):
        """Returns an ion event event_transition that yields to another co-routine.

        If ``annotations`` is not specified, then the ``annotations`` are the annotations of this
        context.
        If ``depth`` is not specified, then the ``depth`` is depth of this context.
        If ``whence`` is not specified, then ``whence`` is the whence of this context.
        """
        if annotations is None:
            annotations = self.annotations
        if annotations is None:
            annotations = ()

        if depth is None:
            depth = self.depth

        if whence is None:
            whence = self.whence

        return Transition(
            event_cls(event_type, ion_type, value, self.field_name, annotations, depth),
            whence
        )

    def immediate_transition(self, delegate=None):
        """Returns an immediate transition to another co-routine.

        If ``delegate`` is not specified, then ``whence`` is the delegate.
        """
        if delegate is None:
            delegate = self.whence

        return Transition(None, delegate)

    def derive_container_context(self, end_sequence, ion_type, add_depth=1,):

        return _HandlerContext(
            end_sequence,
            self.queue,
            self.field_name,
            self.annotations,
            self.depth + add_depth,
            self.whence,
            None,  # containers don't have a value
            ion_type
        )

    def derive_child_context(self, field_name, annotations, whence, value, ion_type):
        return _HandlerContext(
            self.end_sequence,
            self.queue,
            field_name,
            annotations,
            self.depth,
            whence,
            value,
            ion_type
        )

    def derive_ion_type(self, ion_type):
        return _HandlerContext(
            self.end_sequence,
            self.queue,
            self.field_name,
            self.annotations,
            self.depth,
            self.whence,
            self.value,
            ion_type
        )


@coroutine
def _read_data_handler(whence, ctx, stream_event=ION_STREAM_INCOMPLETE_EVENT):
    """Creates a co-routine for retrieving data up to a requested size.

    Args:
        length (int): The minimum length requested.
        whence (Coroutine): The co-routine to return to after the data is satisfied.
        ctx (_HandlerContext): The context for the read.
        skip (Optional[bool]): Whether the requested number of bytes should be skipped.
        stream_event (Optional[IonEvent]): The stream event to return if no bytes are read or
            available.
    """
    trans = None
    queue = ctx.queue

    while True:
        data_event, self = (yield trans)
        if data_event is not None and data_event.data is not None:
            data = data_event.data
            data_len = len(data)
            if data_len > 0:
                # We got something so we can only be incomplete.
                stream_event = ION_STREAM_INCOMPLETE_EVENT
                queue.extend(data)
                yield Transition(None, whence)
        trans = Transition(stream_event, self)

_WHITESPACE = [ord(' '), ord('\t'), ord('\n'), ord('\r')]

_TRIPLE_QUOTE_STRING_SEQUENCE = [(b'\'',), (b'\'',), (b'\'',)]
_SINGLE_QUOTE_STRING_SEQUENCE = [(b'\"',)]
_SINGLE_QUOTE_SYMBOL_SEQUENCE = [(b'\'',)]
_SEXP_END_SEQUENCE = [(b')',)]
_LOB_END_SEQUENCE = [(b'}',), (b'}',)]
_BLOCK_COMMENT_END_SEQUENCE = [(b'*',), (b'/',)]
_LIST_END_SEQUENCE = [(b']',)]
_STRUCT_END_SEQUENCE = [(b'}',)]
_NUMBER_END_SEQUENCE = [(ord('{'), ord('}'), ord('['), ord(']'), ord('('), ord(')'), ord(','), ord('\"'), ord('\''),
                         ord(' '), ord('\t'), ord('\n'), ord('\r'), ord('/'))]  # TODO added '/' for comments -- sexps?


@coroutine
def _container_handler(ctx):
    _, self = (yield None)
    queue = ctx.queue
    c = None
    while True:
        if c is not None and c not in _WHITESPACE:
            # TODO determine how to get this to work in sexps...
            if c == ord('/'):
                handler = comment_handler(c, self)
            else:
                # This is the start of a new child value.
                child_context = ctx.derive_child_context(None, None, self, bytearray(), None)
                handler = get(_START_TABLE, c)(c, child_context)  # Initialize the new handler
            while len(queue) > 0:
                c = queue.read_byte()
                trans = handler.send((c, handler))
                # if trans is not None:  # TODO when would this happen?
                if trans.event is not None:
                    # This child value is finished. c is now the first character in the next value or sequence.
                    # Hence, a new character should not be read; it should be provided to the handler for the next
                    # child context.
                    yield trans
                    break
                else:
                    if self is trans.delegate:
                        # This happens at the end of a comment within this container. Read the next character and
                        # continue.
                        c = queue.read_byte()
                        break
                    # This is either the same handler, or an immediate transition to a new handler if
                    # the type has been narrowed down. In either case, the next character must be read.
                    handler = trans.delegate
            if len(queue) == 0:
                # TODO yield incomplete from read data handler?
                c = None
        elif len(queue) > 0:
            c = queue.read_byte()
        else:
            # This is the top level
            yield Transition(None, _read_data_handler(self, ctx, ION_STREAM_END_EVENT))


def reader(queue=None):
    """Returns a raw binary reader co-routine.

    Args:
        queue (Optional[BufferQueue]): The buffer read data for parsing, if ``None`` a
            new one will be created.

    Yields:
        IonEvent: parse events, will have an event type of ``INCOMPLETE`` if data is needed
            in the middle of a value or ``STREAM_END`` if there is no data **and** the parser
            is not in the middle of parsing a value.

            Receives :class:`DataEvent`, with :class:`ReadEventType` of ``NEXT`` or ``SKIP``
            to iterate over values, or ``DATA`` if the last event was a ``INCOMPLETE``
            or ``STREAM_END`` event type.

            ``SKIP`` is only allowed within a container. A reader is *in* a container
            when the ``CONTAINER_START`` event type is encountered and *not in* a container
            when the ``CONTAINER_END`` event type for that container is encountered.
    """
    if queue is None:
        queue = BufferQueue(text=True)
    ctx = _HandlerContext(
        end_sequence=None,
        queue=queue,
        field_name=None,
        annotations=None,
        depth=0,
        whence=None,
        value=None,
        ion_type=None  # Top level
    )
    return reader_trampoline(_container_handler(ctx))
