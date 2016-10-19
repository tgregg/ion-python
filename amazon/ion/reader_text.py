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

import string
from collections import defaultdict

from amazon.ion.core import Transition, ION_STREAM_INCOMPLETE_EVENT, ION_STREAM_END_EVENT, IonType, IonEvent, \
    IonEventType
from amazon.ion.exceptions import IonException
from amazon.ion.reader import BufferQueue, reader_trampoline
from amazon.ion.util import record, coroutine, Enum


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
    yield ctx.immediate_transition(_NEGATIVE_TABLE[c](c, ctx))


@coroutine
def number_zero_start_handler(c, ctx):
    assert c == ord('0')
    assert len(ctx.value) == 0 or (len(ctx.value) == 1 and ctx.value[0] == ord('-'))
    ctx = ctx.derive_ion_type(IonType.INT)
    ctx.value.append(c)
    c, _ = yield
    if c in _NUMBER_END_SEQUENCE[0]:
        yield ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
    yield ctx.immediate_transition(get(_ZERO_START_TABLE, c)(c, ctx))


@coroutine
def number_or_timestamp_handler(c, ctx):
    assert c in _DIGITS
    ctx = ctx.derive_ion_type(IonType.INT)  # If this is the last digit read, this value is an Int.
    val = ctx.value
    val.append(c)
    c, self = yield
    trans = (Transition(None, self), None)
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
    trans = (Transition(None, self), None)
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
    trans = (Transition(None, self), None)
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
    trans = (Transition(None, self), None)
    negative_exp = False
    while True:
        if c in _NUMBER_END_SEQUENCE[0]:
            if prev == _UNDERSCORE or prev == ord('d') or c == ord('D') or prev == ord('-'):
                raise IonException('%s at end of decimal' % (chr(prev),))
            trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
        else:
            if c == _UNDERSCORE:
                if prev == _UNDERSCORE or prev == ord('d') or c == ord('D') or prev == ord('-'):
                    raise IonException('Underscore after %s' % (chr(prev),))
            else:
                if c == ord('-'):
                    if negative_exp:
                        raise IonException('Multiple negatives in decimal exponent')
                    negative_exp = True
                    val.append(c)
                elif c not in _DIGITS:
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
    trans = (Transition(None, self), None)
    negative_exp = False
    while True:
        if c in _NUMBER_END_SEQUENCE[0]:
            if prev == _UNDERSCORE or prev == ord('e') or c == ord('E') or prev == ord('-'):
                raise IonException('%s at end of float' % (chr(prev),))
            trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
        else:
            if c == _UNDERSCORE:
                if prev == _UNDERSCORE or prev == ord('e') or c == ord('E') or prev == ord('-'):
                    raise IonException('Underscore after %s' % (chr(prev),))
                val.append(c)
            else:
                if c == ord('-'):
                    if negative_exp:
                        raise IonException('Multiple negatives in float exponent')
                    negative_exp = True
                    val.append(c)
                elif c not in _DIGITS:
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
    trans = (Transition(None, self), None)
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
    trans = (Transition(None, self), None)
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
    trans = (Transition(None, self), None)
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
    trans = (Transition(None, self), None)
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
def string_handler(c, ctx, is_field_name=False):
    # TODO parse unicode escapes (in parser)
    assert c == ord('"')
    is_clob = ctx.ion_type is IonType.CLOB
    if not is_clob:
        ctx = ctx.derive_ion_type(IonType.STRING)
    val = ctx.value
    prev = c
    c, self = yield
    trans = (Transition(None, self), None)
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
    if is_field_name:
        ctx = ctx.derive_field_name(val)
        yield ctx.immediate_transition(ctx.whence)
    elif not is_clob:
        yield ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
    else:
        yield ctx.immediate_transition(clob_handler(c, ctx))


class _CommentTransition(Transition):
    """Signals that this transition terminates a comment."""


@coroutine
def comment_handler(c, ctx, whence):
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
    trans = (Transition(None, self), ctx)
    while not done:
        c, _ = yield trans
        if block_comment:
            if prev == ord('*') and c == ord('/'):
                done = True
            prev = c
        else:
            if c == ord('\n'):
                done = True
    yield (_CommentTransition(None, whence), ctx)


@coroutine
def sexp_slash_handler(c, ctx):
    assert c == ord('/')
    c, self = yield
    ctx.queue.unread(c)
    if c == ord('*') or c == ord('/'):
        yield ctx.immediate_transition(comment_handler(ord('/'), ctx, ctx.whence))
    else:
        yield ctx.immediate_transition(operator_symbol_handler(ord('/'), ctx))

@coroutine
def triple_quote_string_handler(c, ctx):
    assert c == ord('\'')
    is_clob = ctx.ion_type is IonType.CLOB
    if not is_clob:
        ctx = ctx.derive_ion_type(IonType.STRING)
    quotes = 0
    in_data = True
    val = ctx.value
    prev = c
    c, self = yield
    here = (Transition(None, self), None)
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
                    ctx.queue.unread(tuple([b'\'']*quotes + [c]))  # un-read the skipped quotes AND c, which will be consumed again later
                    if not is_clob:
                        trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
                    else:
                        trans = ctx.immediate_transition(clob_handler(c, ctx))
                elif c not in _WHITESPACE:
                    if is_clob:
                        trans = ctx.immediate_transition(clob_handler(c, ctx))
                    elif c == ord('/'):
                        trans = ctx.immediate_transition(comment_handler(c, ctx, self))
                    else:
                        trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
        prev = c
        c, _ = yield trans


@coroutine
def string_or_symbol_handler(c, ctx, is_field_name=False):
    assert c == ord('\'')
    ctx = ctx.derive_ion_type(IonType.SYMBOL)
    c, self = yield
    if c == ord('\''):
        yield ctx.immediate_transition(two_single_quotes_handler(c, ctx, is_field_name))
    else:
        yield ctx.immediate_transition(quoted_symbol_handler(c, ctx, is_field_name))


@coroutine
def two_single_quotes_handler(c, ctx, is_field_name):
    assert c == ord('\'')
    c, self = yield
    if c == ord('\''):
        if is_field_name:
            raise IonException("Expected field name, got triple-quoted string")
        yield ctx.immediate_transition(triple_quote_string_handler(c, ctx))
    else:
        # This is the empty symbol
        if c == ord(':'):
            # This is not a value -- it's either an annotation or a field name.
            if not is_field_name:
                c, _ = yield ctx.immediate_transition(self)
                if c == ord(':'):
                    ctx = ctx.derive_annotation(ctx.value)
                else:
                    raise IonException("Illegal character : after symbol")
            else:
                ctx = ctx.derive_field_name(ctx.value)
            yield ctx.immediate_transition(ctx.whence)
        else:
            assert ctx.ion_type == IonType.SYMBOL
            assert len(ctx.value) == 0
            yield ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)


class NullSequence:
    def __init__(self, ion_type, sequence):
        self.ion_type = ion_type
        self.sequence = sequence

    def __getitem__(self, item):
        return self.sequence[item]

_NULL_SEQUENCE = NullSequence(IonType.NULL, tuple(ord(x) for x in 'ull'))
_NULL_SYMBOL_SEQUENCE = NullSequence(IonType.SYMBOL, tuple(ord(x) for x in 'mbol'))
_NULL_SEXP_SEQUENCE = NullSequence(IonType.SEXP, tuple(ord(x) for x in 'xp'))
_NULL_STRING_SEQUENCE = NullSequence(IonType.STRING, tuple(ord(x) for x in 'ng'))
_NULL_STRUCT_SEQUENCE = NullSequence(IonType.STRUCT, tuple(ord(x) for x in 'ct'))
_NULL_INT_SEQUENCE = NullSequence(IonType.INT, tuple(ord(x) for x in 'nt'))
_NULL_FLOAT_SEQUENCE = NullSequence(IonType.FLOAT, tuple(ord(x) for x in 'loat'))
_NULL_DECIMAL_SEQUENCE = NullSequence(IonType.DECIMAL, tuple(ord(x) for x in 'ecimal'))
_NULL_CLOB_SEQUENCE = NullSequence(IonType.CLOB, tuple(ord(x) for x in 'lob'))
_NULL_LIST_SEQUENCE = NullSequence(IonType.LIST, tuple(ord(x) for x in 'ist'))
_NULL_BLOB_SEQUENCE = NullSequence(IonType.BLOB, tuple(ord(x) for x in 'ob'))
_NULL_BOOL_SEQUENCE = NullSequence(IonType.BOOL, tuple(ord(x) for x in 'ol'))
_NULL_TIMESTAMP_SEQUENCE = NullSequence(IonType.TIMESTAMP, tuple(ord(x) for x in 'imestamp'))

_NULL_STR_NEXT = {
    ord('i'): _NULL_STRING_SEQUENCE,
    ord('u'): _NULL_STRUCT_SEQUENCE
}

_NULL_ST_NEXT = {
    ord('r'): _NULL_STR_NEXT
}

_NULL_S_NEXT = {
    ord('y'): _NULL_SYMBOL_SEQUENCE,
    ord('e'): _NULL_SEXP_SEQUENCE,
    ord('t'): _NULL_ST_NEXT
}

_NULL_B_NEXT = {
    ord('l'): _NULL_BLOB_SEQUENCE,
    ord('o'): _NULL_BOOL_SEQUENCE
}

_NULL_STARTS = {
    ord('n'): _NULL_SEQUENCE,  # null.null
    ord('s'): _NULL_S_NEXT, # null.string, null.symbol, null.struct, null.sexp
    ord('i'): _NULL_INT_SEQUENCE,  # null.int
    ord('f'): _NULL_FLOAT_SEQUENCE,  # null.float
    ord('d'): _NULL_DECIMAL_SEQUENCE,  # null.decimal
    ord('b'): _NULL_B_NEXT,  # null.bool, null.blob
    ord('c'): _NULL_CLOB_SEQUENCE,  # null.clob
    ord('l'): _NULL_LIST_SEQUENCE,  # null.list
    ord('t'): _NULL_TIMESTAMP_SEQUENCE,  # null.timestamp
}


@coroutine
def null_handler(c, ctx):
    assert c == ord('.')
    c, self = yield
    nxt = _NULL_STARTS
    i = 0
    length = None
    done = False
    trans = (Transition(None, self), None)
    while True:
        if done:
            if c in _NUMBER_END_SEQUENCE[0] or (ctx.container.ion_type is IonType.SEXP and c in _OPERATORS):  # TODO CHECK THIS
                trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, nxt.ion_type, None)
            else:
                raise IonException("Illegal character %s in null type" % (chr(c), ))
        elif length is None:
            if c not in nxt:
                raise IonException("Illegal character %s in null type" % (chr(c), ))
            nxt = nxt[c]
            if isinstance(nxt, NullSequence):
                length = len(nxt.sequence)
        else:
            if c != nxt[i]:
                raise IonException("Illegal character %s in null type" % (chr(c), ))
            i += 1
            done = i == length
        c, _ = yield trans


_TRUE_SEQUENCE = tuple(ord(x) for x in 'rue')
_FALSE_SEQUENCE = tuple(ord(x) for x in 'alse')


@coroutine
def symbol_or_null_or_bool_handler(c, ctx, is_field_name=False):
    in_sexp = ctx.container.ion_type is IonType.SEXP
    if c not in _IDENTIFIER_STARTS:
        if in_sexp and c in _OPERATORS:
            c_next, _ = yield
            ctx.queue.unread(c_next)
            yield ctx.immediate_transition(operator_symbol_handler(c, ctx))
        raise IonException("Illegal character %s in symbol." % (chr(c), ))  # TODO
    val = ctx.value
    val.append(c)
    maybe_null = c == ord('n')
    maybe_true = c == ord('t')
    maybe_false = c == ord('f')
    c, self = yield
    trans = (Transition(None, self), None)
    match_index = 0
    while True:
        if maybe_null:
            if match_index < len(_NULL_SEQUENCE.sequence):
                maybe_null = c == _NULL_SEQUENCE[match_index]
            else:
                if c in _WHITESPACE or c == ord('/') or c in ctx.container.delimiter or c in ctx.container.end_sequence:
                    if is_field_name:
                        raise IonException("Null field name not allowed")
                    yield ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.NULL, None)
                elif c == ord('.'):
                    if is_field_name:
                        raise IonException("Illegal character in field name: .")  # TODO
                    yield ctx.immediate_transition(null_handler(c, ctx))
                elif c == ord(':'):
                    if is_field_name:
                        raise IonException("Null field name not allowed")
                    else:
                        raise IonException("Illegal character in symbol: :")  # TODO
                elif in_sexp and c in _OPERATORS:
                    yield ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.NULL, None)
                else:
                    maybe_null = False
        elif maybe_true:
            if match_index < len(_TRUE_SEQUENCE):
                maybe_true = c == _TRUE_SEQUENCE[match_index]
            else:
                if c in _WHITESPACE or c == ord('/') or c in ctx.container.delimiter or c in ctx.container.end_sequence:
                    if is_field_name:
                        raise IonException("true keyword as field name not allowed")
                    yield ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.BOOL, True)
                elif c == ord(':'):
                    if is_field_name:
                        raise IonException("true keyword as field name not allowed")
                    else:
                        raise IonException("Illegal character in symbol: :")  # TODO
                elif in_sexp and c in _OPERATORS:
                    yield ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.BOOL, True)
                else:
                    maybe_true = False
        elif maybe_false:
            if match_index < len(_FALSE_SEQUENCE):
                maybe_false = c == _FALSE_SEQUENCE[match_index]
            else:
                if c in _WHITESPACE or c == ord('/') or c in ctx.container.delimiter or c in ctx.container.end_sequence:
                    if is_field_name:
                        raise IonException("false keyword as field name not allowed")
                    yield ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.BOOL, False)
                elif c == ord(':'):
                    if is_field_name:
                        raise IonException("false keyword as field name not allowed")
                    else:
                        raise IonException("Illegal character in symbol: :")  # TODO
                elif in_sexp and c in _OPERATORS:
                    yield ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.BOOL, False)
                else:
                    maybe_false = False
        if maybe_null or maybe_true or maybe_false:
            val.append(c)
            match_index += 1
        else:
            ctx = ctx.derive_ion_type(IonType.SYMBOL)
            if c in _WHITESPACE or c == ord('/') or c == ord(':'):
                if is_field_name:
                    ctx = ctx.derive_field_name(val)
                # This might be an annotation
                trans = ctx.immediate_transition(ctx.whence)
            elif c in ctx.container.end_sequence or c in ctx.container.delimiter or (in_sexp and c in _OPERATORS):
                trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL, val)
            else:
                trans = ctx.immediate_transition(symbol_handler(c, ctx, is_field_name=is_field_name))
        c, _ = yield trans


@coroutine
def sexp_hyphen_handler(c, ctx):
    assert ctx.value[0] == ord('-')
    assert c not in _DIGITS
    ctx.queue.unread(c)
    yield
    if ctx.container.ion_type is not IonType.SEXP:
        raise IonException("Illegal character following -")  # TODO
    if c in _OPERATORS:
        yield ctx.immediate_transition(operator_symbol_handler(c, ctx))
    else:
        yield ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL, ctx.value)


@coroutine
def operator_symbol_handler(c, ctx):
    assert c in _OPERATORS
    val = ctx.value
    val.append(c)
    c, self = yield
    trans = (Transition(None, self), None)
    while c in _OPERATORS:
        val.append(c)
        c, _ = yield trans
    yield ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL, val)


@coroutine
def quoted_symbol_handler(c, ctx, is_field_name):
    assert c != ord('\'')
    val = ctx.value
    val.append(c)
    prev = c
    c, self = yield
    trans = (Transition(None, self), None)
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
    if is_field_name:
        ctx = ctx.derive_field_name(val)
        yield ctx.immediate_transition(ctx.whence)
    elif c in _WHITESPACE or c == ord('/') or c == ord(':'):
        # This might be an annotation.
        yield ctx.immediate_transition(ctx.whence)
    else:
        yield ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)


_DIGITS = tuple([ord(x) for x in string.digits])

_IDENTIFIER_STARTS = tuple([ord(x) for x in string.ascii_letters]) + (ord('$'), ord('_'))
_IDENTIFIER_CHARACTERS = _IDENTIFIER_STARTS + _DIGITS


@coroutine
def symbol_handler(c, ctx, is_field_name=False):
    assert c in _IDENTIFIER_CHARACTERS
    in_sexp = ctx.container.ion_type is IonType.SEXP
    ctx = ctx.derive_ion_type(IonType.SYMBOL)
    val = ctx.value
    val.append(c)
    prev = c
    c, self = yield
    trans = (Transition(None, self), None)
    while True:
        if c not in _WHITESPACE:
            if prev in _WHITESPACE or c in _NUMBER_END_SEQUENCE[0] or c == ord(':') or (in_sexp and c in _OPERATORS):
                break
            if c not in _IDENTIFIER_CHARACTERS:
                raise IonException("Illegal character %s in symbol" % (chr(c), ))  # TODO
            val.append(c)
        prev = c
        c, _ = yield trans
    if is_field_name:
        ctx = ctx.derive_field_name(val)
        yield ctx.immediate_transition(ctx.whence)
    elif c in _WHITESPACE or c == ord('/') or c == ord(':'):
        # This might be an annotation.
        yield ctx.immediate_transition(ctx.whence)
    else:
        yield ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)


@coroutine
def struct_or_lob_handler(c, ctx):
    assert c == ord('{')
    c, self = yield
    yield ctx.immediate_transition(_STRUCT_OR_LOB_TABLE[c](c, ctx))


@coroutine
def lob_handler(c, ctx):
    assert c == ord('{')
    c, self = yield
    trans = (Transition(None, self), None)
    quotes = 0
    while True:
        if c in _WHITESPACE:
            if quotes > 0:
                raise IonException("Illegal character ' in blob")
        elif c == ord('"'):
            if quotes > 0:
                raise IonException("Illegal character in clob")  # TODO
            ctx = ctx.derive_ion_type(IonType.CLOB)
            yield ctx.immediate_transition(string_handler(c, ctx))
        elif c == ord('\''):
            if not quotes:
                ctx = ctx.derive_ion_type(IonType.CLOB)
            quotes += 1
            if quotes == 3:
                yield ctx.immediate_transition(triple_quote_string_handler(c, ctx))
        else:
            yield ctx.immediate_transition(blob_handler(c, ctx))
        c, _ = yield trans


@coroutine
def blob_handler(c, ctx):
    # Note: all validation of base 64 characters will be left to the base64 library in the parsing phase. It could
    # be partly done here in the lexer by checking each character against the base64 alphabet and making sure the blob
    # ends with the correct number of '=', but this is simpler.
    val = ctx.value
    if c != ord('}') and c not in _WHITESPACE:
        val.append(c)
    prev = c
    c, self = yield
    trans = (Transition(None, self), None)
    done = False
    while not done:
        if c in _WHITESPACE:
            if prev == ord('}'):
                raise IonException("Illegal character in blob; expected }")  # TODO
        else:
            if c != ord('}'):
                val.append(c)
            elif prev == ord('}'):
                done = True
        prev = c
        c, _ = yield trans
    yield ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.BLOB, val)


@coroutine
def clob_handler(c, ctx):
    if c != ord('}') and c not in _WHITESPACE:
        raise IonException("Illegal character in clob")  # TODO
    prev = c
    c, self = yield
    trans = (Transition(None, self), None)
    done = False
    while not done:
        if c in _WHITESPACE:
            if prev == ord('}'):
                raise IonException("Illegal character in blob; expected }")  # TODO
        elif c == ord('}'):
            if prev == ord('}'):
                done = True
        else:
            raise IonException("Illegal character in clob")  # TODO
        prev = c
        c, _ = yield trans
    yield ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.CLOB, ctx.value)


@coroutine
def field_name_handler(c, ctx):
    yield
    ctx.queue.unread(c)
    if c == ord('\''):
        trans = ctx.immediate_transition(string_or_symbol_handler(c, ctx, is_field_name=True))
    elif c == ord('"'):
        trans = ctx.immediate_transition(string_handler(c, ctx, is_field_name=True))
    elif c in _IDENTIFIER_STARTS:
        trans = ctx.immediate_transition(symbol_or_null_or_bool_handler(c, ctx, is_field_name=True))
    else:
        raise IonException("Illegal character %s in field name." % (chr(c), ))
    yield trans


@coroutine
def struct_handler(c, ctx):
    ctx.queue.unread(c)  # Necessary because we had to read one char past the { to make sure this isn't a lob
    yield
    yield ctx.event_transition(IonEvent, IonEventType.CONTAINER_START, IonType.STRUCT)


@coroutine
def list_handler(c, ctx):
    yield
    yield ctx.event_transition(IonEvent, IonEventType.CONTAINER_START, IonType.LIST)


@coroutine
def sexp_handler(c, ctx):
    yield
    yield ctx.event_transition(IonEvent, IonEventType.CONTAINER_START, IonType.SEXP)


_BINARY_DIGITS = (
    ord('0'),
    ord('1')
)

_HEX_DIGITS = _DIGITS + tuple(ord(x) for x in 'abcdef')

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

_NEGATIVE_TABLE = defaultdict(
    lambda: sexp_hyphen_handler
)
_NEGATIVE_TABLE[ord('0')] = number_zero_start_handler
_NEGATIVE_TABLE[ord('1')] = number_handler
_NEGATIVE_TABLE[ord('2')] = number_handler
_NEGATIVE_TABLE[ord('3')] = number_handler
_NEGATIVE_TABLE[ord('4')] = number_handler
_NEGATIVE_TABLE[ord('5')] = number_handler
_NEGATIVE_TABLE[ord('6')] = number_handler
_NEGATIVE_TABLE[ord('7')] = number_handler
_NEGATIVE_TABLE[ord('8')] = number_handler
_NEGATIVE_TABLE[ord('9')] = number_handler

_STRUCT_OR_LOB_TABLE = defaultdict(
    lambda: struct_handler
)
_STRUCT_OR_LOB_TABLE[ord('{')] = lob_handler

_START_TABLE = defaultdict(
    lambda: symbol_or_null_or_bool_handler
)
_START_TABLE[ord('-')] = number_negative_start_handler
_START_TABLE[ord('0')] = number_zero_start_handler
_START_TABLE[ord('1')] = number_or_timestamp_handler
_START_TABLE[ord('2')] = number_or_timestamp_handler
_START_TABLE[ord('3')] = number_or_timestamp_handler
_START_TABLE[ord('4')] = number_or_timestamp_handler
_START_TABLE[ord('5')] = number_or_timestamp_handler
_START_TABLE[ord('6')] = number_or_timestamp_handler
_START_TABLE[ord('7')] = number_or_timestamp_handler
_START_TABLE[ord('8')] = number_or_timestamp_handler
_START_TABLE[ord('9')] = number_or_timestamp_handler
_START_TABLE[ord('{')] = struct_or_lob_handler
_START_TABLE[ord('(')] = sexp_handler
_START_TABLE[ord('[')] = list_handler
_START_TABLE[ord('\'')] = string_or_symbol_handler
_START_TABLE[ord('\"')] = string_handler
_START_TABLE[ord('/')] = sexp_slash_handler  # Comments are handled as a special case, not through _START_TABLE

_WHITESPACE = (ord(' '), ord('\t'), ord('\n'), ord('\r'))

_NUMBER_END_SEQUENCE = [(ord('{'), ord('}'), ord('['), ord(']'), ord('('), ord(')'), ord(','), ord('\"'), ord('\''),
                         ord(' '), ord('\t'), ord('\n'), ord('\r'), ord('/'))]

_OPERATORS = tuple([ord(x) for x in '!#%&*+\-./;<=>?@^`|~'])  # TODO is backslash allowed? Spec: no, java: no, grammar: yes


class _Container(record(
    'end_sequence', 'delimiter', 'ion_type'
)):
    """TODO"""

_C_TOP_LEVEL = _Container((), (), None)
_C_STRUCT = _Container((ord('}'),), (ord(','),), IonType.STRUCT)
_C_LIST = _Container((ord(']'),), (ord(','),), IonType.LIST)
_C_SEXP = _Container((ord(')'),), (), IonType.SEXP)


class _HandlerContext(record(
    'container', 'queue', 'field_name', 'annotations', 'depth', 'whence', 'value', 'ion_type'
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
        ), None

    def immediate_transition(self, delegate=None):
        """Returns an immediate transition to another co-routine.

        If ``delegate`` is not specified, then ``whence`` is the delegate.
        """
        if delegate is None:
            delegate = self.whence

        return Transition(None, delegate), self

    def derive_container_context(self, ion_type, whence, add_depth=1):
        if ion_type is IonType.STRUCT:
            container = _C_STRUCT
        elif ion_type is IonType.LIST:
            container = _C_LIST
        elif ion_type is IonType.SEXP:
            container = _C_SEXP
        else:
            raise TypeError("Cannot derive container context for non-container type")  # TODO
        return _HandlerContext(
            container,
            self.queue,
            self.field_name,
            self.annotations,
            self.depth + add_depth,
            whence,
            None,  # containers don't have a value
            ion_type
        )

    def derive_child_context(self, field_name, annotations, whence, value, ion_type):
        return _HandlerContext(
            self.container,
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
            self.container,
            self.queue,
            self.field_name,
            self.annotations,
            self.depth,
            self.whence,
            self.value,
            ion_type
        )

    def derive_annotation(self, annotation):
        return _HandlerContext(
            self.container,
            self.queue,
            self.field_name,
            self.annotations and self.annotations + (annotation, ) or (annotation, ),
            self.depth,
            self.whence,
            bytearray(),
            None
        )

    def derive_field_name(self, field_name):
        return _HandlerContext(
            self.container,
            self.queue,
            field_name,
            self.annotations,
            self.depth,
            self.whence,
            bytearray(),
            self.ion_type
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


@coroutine
def _container_handler(c, ctx):
    _, self = (yield None)
    queue = ctx.queue
    child_context = None
    is_field_name = True
    delimiter_required = False
    maybe_annotation = False
    while True:
        if c in ctx.container.end_sequence:
            if child_context and child_context.ion_type is IonType.SYMBOL:
                yield child_context.event_transition(IonEvent, IonEventType.SCALAR, child_context.ion_type, child_context.value)[0]
            # We are at the end of the container.
            # Yield the close event and go to enclosing container.
            yield Transition(
                IonEvent(IonEventType.CONTAINER_END, ctx.ion_type, depth=ctx.depth-1),
                ctx.whence
            )
        if c in ctx.container.delimiter:
            if not delimiter_required:
                raise IonException('Encountered delimiter %s without preceding value.' % (chr(ctx.container.delimiter[0])))
            is_field_name = True
            delimiter_required = False
            c = None
        if c is not None and c not in _WHITESPACE:
            if c == ord('/'):
                if child_context is None:
                    # TODO duplicated in a branch below
                    # This is the start of a new child value.
                    child_context = ctx.derive_child_context(None, None, self, bytearray(), None)
                if ctx.ion_type is IonType.SEXP:
                    handler = sexp_slash_handler(c, child_context)
                else:
                    handler = comment_handler(c, child_context, self)
            elif delimiter_required:
                # This is not the delimiter, or whitespace, or the start of a comment. Throw.
                raise IonException("Delimiter %s not found after value within %s." % (
                    chr(ctx.container.delimiter[0]), ctx.container.ion_type.name))
            elif c == ord(':') and is_field_name and ctx.ion_type is IonType.STRUCT:
                is_field_name = False
                c = None
                continue
            else:
                if child_context is not None and child_context.value and child_context.ion_type is IonType.SYMBOL:
                    if c == ord(':'):
                        # Note: field name covered in a previous branch, so not seeing another : here is definitely
                        # an error.
                        if len(queue) == 0:
                            yield Transition(None, _read_data_handler(self, ctx))
                        peek = queue.read_byte()
                        if peek == ord(':'):
                            child_context = child_context.derive_annotation(child_context.value)
                            c = None  # forces another character to be read safely
                            continue
                        else:
                            raise IonException("Illegal character : in symbol")
                    else:
                        yield child_context.event_transition(IonEvent, IonEventType.SCALAR, child_context.ion_type,
                                                             child_context.value)[0]
                        maybe_annotation = False
                        child_context = None
                if child_context is None or (not child_context.annotations and child_context.field_name is None):
                    # This is the start of a new child value.
                    child_context = ctx.derive_child_context(None, None, self, bytearray(), None)
                if is_field_name and ctx.ion_type is IonType.STRUCT:
                    handler = field_name_handler(c, child_context)
                else:
                    handler = _START_TABLE[c](c, child_context)  # Initialize the new handler
            container_start = c == ord(b'[') or c == ord(b'(')  # Note: '{' not here because that might be a lob
            read_next = True
            while True:
                if not container_start:
                    if len(queue) == 0:
                        yield Transition(None, _read_data_handler(self, ctx))
                    c = queue.read_byte()
                else:
                    c = None
                    container_start = False
                trans, child_context = handler.send((c, handler))
                if trans.event is not None:
                    # This child value is finished. c is now the first character in the next value or sequence.
                    # Hence, a new character should not be read; it should be provided to the handler for the next
                    # child context.
                    yield trans
                    maybe_annotation = False
                    if trans.event.ion_type.is_container and trans.event.event_type is not IonEventType.SCALAR:
                        yield Transition(
                            None,
                            _container_handler(c, ctx.derive_container_context(trans.event.ion_type, self))
                        )
                        # The end of the container has been reached, and c needs to be updated
                        if len(queue) > 0:
                            c = queue.read_byte()
                            read_next = False
                    else:
                        read_next = False
                    delimiter_required = not ((not ctx.container.ion_type) or ctx.container.ion_type is IonType.SEXP)
                    break
                else:
                    if self is trans.delegate:
                        in_comment = isinstance(trans, _CommentTransition)
                        if is_field_name and ctx.ion_type is IonType.STRUCT:
                            if c == ord(':'):
                                is_field_name = False
                            elif not (c == ord('/') and in_comment):
                                break
                        elif child_context and child_context.ion_type is IonType.SYMBOL:
                            maybe_annotation = True
                            if not in_comment:
                                break
                        # This happens at the end of a comment within this container, or when an annotation has been
                        # found. In both cases, an event should not be emitted. Read the next character and continue.
                        if len(queue) > 0:
                            c = queue.read_byte()
                            read_next = False
                        break
                    # This is either the same handler, or an immediate transition to a new handler if
                    # the type has been narrowed down. In either case, the next character must be read.
                    handler = trans.delegate
            if read_next and len(queue) == 0:
                # This will cause the next loop to fall through to the else branch below to ask for more input.
                c = None
        elif len(queue) > 0:
            c = queue.read_byte()
        else:
            if ctx.depth == 0 and not maybe_annotation:
                yield Transition(None, _read_data_handler(self, ctx, ION_STREAM_END_EVENT))
            else:
                yield Transition(None, _read_data_handler(self, ctx))


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
        queue = BufferQueue()
    ctx = _HandlerContext(
        _C_TOP_LEVEL,
        queue=queue,
        field_name=None,
        annotations=None,
        depth=0,
        whence=None,
        value=None,
        ion_type=None  # Top level
    )
    return reader_trampoline(_container_handler(None, ctx))
