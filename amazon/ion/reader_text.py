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

import six

from amazon.ion.core import Transition, ION_STREAM_INCOMPLETE_EVENT, ION_STREAM_END_EVENT, IonType, IonEvent, \
    IonEventType
from amazon.ion.exceptions import IonException
from amazon.ion.reader import BufferQueue, reader_trampoline
from amazon.ion.util import record, coroutine, Enum


def _illegal_character(c, ctx):
    container_type = ctx.container.ion_type is None and 'top-level' or ctx.container.ion_type.name
    value_type = ctx.ion_type is None and 'unknown' or ctx.ion_type.name
    raise IonException('Illegal character %s at position %d in %s value contained in %s.'
                       % (chr(c), ctx.queue.position, value_type, container_type))


def _whitelist(dct, fallback=_illegal_character):
    out = defaultdict(lambda: fallback)
    for k, v in six.iteritems(dct):
        out[k] = v
    return out


def _seq(s):
    return tuple(ord(x) for x in s)


_WHITESPACE = _seq(' \t\n\r')
_NUMBER_TERMINATORS = _seq('{}[](),\"\' \t\n\r/')
_DIGITS = _seq(string.digits)
_BINARY_DIGITS = _seq('01')
_HEX_DIGITS = _DIGITS + _seq('abcdef')
_DECIMAL_EXPS = _seq('Dd')
_FLOAT_EXPS = _seq('Ee')
_TIMESTAMP_DELIMITERS = _seq('-:+.')
_TIMESTAMP_OFFSET_INDICATORS = _seq('Z+-')
_IDENTIFIER_STARTS = _seq(string.ascii_letters) + (_seq('$_'))
_IDENTIFIER_CHARACTERS = _IDENTIFIER_STARTS + _DIGITS
_OPERATORS = _seq('!#%&*+\-./;<=>?@^`|~')  # TODO is backslash allowed? Spec: no, java: no, grammar: yes

_UNDERSCORE = ord('_')

_TRUE_SEQUENCE = _seq('rue')
_FALSE_SEQUENCE = _seq('alse')
_NAN_SEQUENCE = _seq('an')
_INF_SEQUENCE = _seq('inf')


class NullSequence:
    def __init__(self, ion_type, sequence):
        self.ion_type = ion_type
        self.sequence = sequence

    def __getitem__(self, item):
        return self.sequence[item]

_NULL_SEQUENCE = NullSequence(IonType.NULL, _seq('ull'))
_NULL_SYMBOL_SEQUENCE = NullSequence(IonType.SYMBOL, _seq('mbol'))
_NULL_SEXP_SEQUENCE = NullSequence(IonType.SEXP, _seq('xp'))
_NULL_STRING_SEQUENCE = NullSequence(IonType.STRING, _seq('ng'))
_NULL_STRUCT_SEQUENCE = NullSequence(IonType.STRUCT, _seq('ct'))
_NULL_INT_SEQUENCE = NullSequence(IonType.INT, _seq('nt'))
_NULL_FLOAT_SEQUENCE = NullSequence(IonType.FLOAT, _seq('loat'))
_NULL_DECIMAL_SEQUENCE = NullSequence(IonType.DECIMAL, _seq('ecimal'))
_NULL_CLOB_SEQUENCE = NullSequence(IonType.CLOB, _seq('lob'))
_NULL_LIST_SEQUENCE = NullSequence(IonType.LIST, _seq('ist'))
_NULL_BLOB_SEQUENCE = NullSequence(IonType.BLOB, _seq('ob'))
_NULL_BOOL_SEQUENCE = NullSequence(IonType.BOOL, _seq('ol'))
_NULL_TIMESTAMP_SEQUENCE = NullSequence(IonType.TIMESTAMP, _seq('imestamp'))

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


class _Container(record(
    'end_sequence', 'delimiter', 'ion_type'
)):
    """TODO"""

_C_TOP_LEVEL = _Container((), (), None)
_C_STRUCT = _Container((ord('}'),), (ord(','),), IonType.STRUCT)
_C_LIST = _Container((ord(']'),), (ord(','),), IonType.LIST)
_C_SEXP = _Container((ord(')'),), (), IonType.SEXP)


class _HandlerContext(record(
    'container', 'queue', 'field_name', 'annotations', 'depth', 'whence', 'value', 'ion_type', 'pending_symbol'
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
            ion_type,
            None
        )

    def derive_child_context(self, whence):
        return _HandlerContext(
            self.container,
            self.queue,
            None,
            None,
            self.depth,
            whence,
            bytearray(),  # children start without a value
            None,
            None
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
            ion_type,
            self.pending_symbol
        )

    def derive_annotation(self):
        assert self.pending_symbol is not None
        assert not self.value
        annotations = (self.pending_symbol, )  # pending_symbol becomes an annotation
        return _HandlerContext(
            self.container,
            self.queue,
            self.field_name,
            self.annotations and self.annotations + annotations or annotations,
            self.depth,
            self.whence,
            self.value,
            None,
            None  # reset pending symbol
        )

    def derive_field_name(self):
        assert self.pending_symbol is not None
        assert not self.value
        return _HandlerContext(
            self.container,
            self.queue,
            self.pending_symbol,  # pending_symbol becomes field name
            self.annotations,
            self.depth,
            self.whence,
            self.value,
            self.ion_type,
            None  # reset pending symbol
        )

    def derive_pending_symbol(self, pending_symbol=bytearray()):
        return _HandlerContext(
            self.container,
            self.queue,
            self.field_name,
            self.annotations,
            self.depth,
            self.whence,
            bytearray(),  # reset value
            self.ion_type,
            pending_symbol
        )


class _CommentTransition(Transition):
    """Signals that this transition terminates a comment."""


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
    if c in _NUMBER_TERMINATORS:
        yield ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
    yield ctx.immediate_transition(_ZERO_START_TABLE[c](c, ctx))


@coroutine
def number_or_timestamp_handler(c, ctx):
    assert c in _DIGITS
    ctx = ctx.derive_ion_type(IonType.INT)  # If this is the last digit read, this value is an Int.
    val = ctx.value
    val.append(c)
    c, self = yield
    trans = (Transition(None, self), None)
    while True:
        if c in _NUMBER_TERMINATORS:
            trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
        else:
            if c not in _DIGITS:
                trans = ctx.immediate_transition(_NUMBER_OR_TIMESTAMP_TABLE[c](c, ctx))
            else:
                val.append(c)
        c, _ = yield trans


@coroutine
def number_handler(c, ctx):
    val = ctx.value
    if c != _UNDERSCORE:
        val.append(c)
    prev = c
    c, self = yield
    trans = (Transition(None, self), None)
    while True:
        if c in _NUMBER_TERMINATORS:
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
        if c in _NUMBER_TERMINATORS:
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
                    trans = ctx.immediate_transition(_REAL_NUMBER_TABLE[c](c, ctx))
                else:
                    val.append(c)
        prev = c
        c, _ = yield trans


def _generate_exponent_handler(ion_type, exp_chars):
    @coroutine
    def exponent_handler(c, ctx):
        assert c in exp_chars
        ctx = ctx.derive_ion_type(ion_type)
        val = ctx.value
        val.append(c)
        prev = c
        c, self = yield
        if c == _UNDERSCORE:
            raise IonException('Underscore after exponent')
        trans = (Transition(None, self), None)
        negative_exp = False
        while True:
            if c in _NUMBER_TERMINATORS:
                if prev == _UNDERSCORE or prev in exp_chars or prev == ord('-'):
                    raise IonException('%s at end of real number' % (chr(prev),))
                trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
            else:
                if c == _UNDERSCORE:
                    if prev == _UNDERSCORE or prev in exp_chars or prev == ord('-'):
                        raise IonException('Underscore after %s' % (chr(prev),))
                else:
                    if c == ord('-'):
                        if negative_exp:
                            raise IonException('Multiple negatives in exponent')
                        negative_exp = True
                        val.append(c)
                    elif c not in _DIGITS:
                        _illegal_character(c, ctx)
                    else:
                        val.append(c)
            prev = c
            c, _ = yield trans
    return exponent_handler


decimal_handler = _generate_exponent_handler(IonType.DECIMAL, _DECIMAL_EXPS)
float_handler = _generate_exponent_handler(IonType.FLOAT, _FLOAT_EXPS)


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
        if c in _NUMBER_TERMINATORS:
            if prev == _UNDERSCORE or prev == ord('b') or prev == ord('B'):
                raise IonException('%s at end of binary int' % (chr(prev),))
            trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
        else:
            if c == _UNDERSCORE:
                if prev == _UNDERSCORE or prev == ord('b') or prev == ord('B'):
                    raise IonException('Underscore after %s' % (chr(prev),))
            else:
                if c not in _BINARY_DIGITS:
                    _illegal_character(c, ctx)
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
        if c in _NUMBER_TERMINATORS:
            if prev == _UNDERSCORE or prev == ord('x') or prev == ord('X'):
                raise IonException('%s at end of hex int' % (chr(prev),))
            trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
        else:
            if c == _UNDERSCORE:
                if prev == _UNDERSCORE or prev == ord('x') or prev == ord('X'):
                    raise IonException('Underscore after %s' % (chr(prev),))
            else:
                if c not in _HEX_DIGITS:
                    _illegal_character(c, ctx)
                else:
                    val.append(c)
        prev = c
        c, _ = yield trans


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
        nxt += _NUMBER_TERMINATORS
    while True:
        if c not in nxt:
            raise IonException("Illegal character %s in timestamp; expected %r in state %r "
                               % (chr(c), [chr(x) for x in nxt], state))
        if c in _NUMBER_TERMINATORS:
            trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
        else:
            if c == ord('Z'):
                nxt = _NUMBER_TERMINATORS
            elif c == ord('T'):
                nxt = _NUMBER_TERMINATORS + _DIGITS
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
                        nxt = (ord('T'),) + _NUMBER_TERMINATORS
                    elif state == State.HOUR:
                        nxt = (ord(':'),)
                    elif state == State.MINUTE:
                        nxt = _TIMESTAMP_OFFSET_INDICATORS + (ord(':'),)
                    elif state == State.SECOND:
                        nxt = _TIMESTAMP_OFFSET_INDICATORS + (ord('.'),)
                    elif state == State.FRACTIONAL:
                        nxt = _DIGITS + _TIMESTAMP_OFFSET_INDICATORS + (ord('.'),)
                    elif state == State.OFF_HOUR:
                        nxt = (ord(':'),) + _NUMBER_TERMINATORS
                    elif state == State.OFF_MINUTE:
                        nxt = _NUMBER_TERMINATORS
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
    if not is_clob and not is_field_name:
        ctx = ctx.derive_ion_type(IonType.STRING)
    val = ctx.value
    if is_field_name:
        assert not val
        ctx = ctx.derive_pending_symbol()
        val = ctx.pending_symbol
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
        yield ctx.immediate_transition(ctx.whence)
    elif not is_clob:
        yield ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
    else:
        yield ctx.immediate_transition(clob_handler(c, ctx))


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
    #ctx = ctx.derive_ion_type(IonType.SYMBOL)
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
        ctx = ctx.derive_pending_symbol()
        yield ctx.immediate_transition(ctx.whence)


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
            if c in _NUMBER_TERMINATORS or (ctx.container.ion_type is IonType.SEXP and c in _OPERATORS):  # TODO CHECK THIS
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


@coroutine
def symbol_or_keyword_handler(c, ctx, is_field_name=False):
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
    maybe_nan = maybe_null
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
        if maybe_nan:
            if match_index < len(_NAN_SEQUENCE):
                maybe_nan = c == _NAN_SEQUENCE[match_index]
            else:
                if c in _WHITESPACE or c == ord('/') or c in ctx.container.delimiter or c in ctx.container.end_sequence:
                    if is_field_name:
                        raise IonException("nan keyword as field name not allowed")
                    yield ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.FLOAT, val)
                elif c == ord(':'):
                    if is_field_name:
                        raise IonException("nan keyword as field name not allowed")
                    else:
                        raise IonException("Illegal character in symbol: :")  # TODO
                elif in_sexp and c in _OPERATORS:
                    yield ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.FLOAT, val)
                else:
                    maybe_nan = False
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
        if maybe_null or maybe_nan or maybe_true or maybe_false:
            val.append(c)
            match_index += 1
        else:
            if c in _WHITESPACE or c == ord('/') or c == ord(':'):
                # This might be an annotation or a field name
                ctx = ctx.derive_pending_symbol(val)
                trans = ctx.immediate_transition(ctx.whence)
            elif c in ctx.container.end_sequence or c in ctx.container.delimiter or (in_sexp and c in _OPERATORS):
                ctx = ctx.derive_ion_type(IonType.SYMBOL)
                trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL, val)
            else:
                trans = ctx.immediate_transition(symbol_handler(c, ctx, is_field_name=is_field_name))
        c, _ = yield trans


def _generate_inf_or_operator_handler(c_start, is_delegate=True):
    @coroutine
    def inf_or_operator_handler(c, ctx):
        if not is_delegate:
            ctx.value.append(c_start)
            c, self = yield
        else:
            assert ctx.value[0] == ord(c_start)
            assert c not in _DIGITS
            ctx.queue.unread(c)
            next_ctx = ctx
            _, self = yield
            assert c == _
        maybe_inf = True
        match_index = 0
        trans = (Transition(None, self), None)
        while True:
            if maybe_inf:
                if match_index < len(_INF_SEQUENCE):
                    maybe_inf = c == _INF_SEQUENCE[match_index]
                else:
                    if c in _NUMBER_TERMINATORS or (ctx.container.ion_type is IonType.SEXP and c in _OPERATORS):
                        yield ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.FLOAT, bytearray(c_start + b'inf'))
                    else:
                        maybe_inf = False
            if maybe_inf:
                match_index += 1
            else:
                if match_index > 0:
                    next_ctx = ctx.derive_child_context(ctx.whence)
                    for ch in _INF_SEQUENCE[0:match_index]:
                        next_ctx.value.append(ch)
                break
            c, self = yield trans
        if ctx.container.ion_type is not IonType.SEXP:
            raise IonException("Illegal character %s following %s" % (chr(c), c_start))  # TODO
        if match_index == 0:
            if c in _OPERATORS:
                yield ctx.immediate_transition(operator_symbol_handler(c, ctx))
            yield ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL, ctx.value)
        next_immediate = next_ctx.immediate_transition(symbol_handler(c, next_ctx))
        ctx = ctx.derive_ion_type(IonType.SYMBOL)
        yield [ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL, ctx.value)[0]] \
              + [next_immediate[0]], next_ctx
    return inf_or_operator_handler


negative_inf_or_sexp_hyphen_handler = _generate_inf_or_operator_handler(b'-')
positive_inf_or_sexp_plus_handler = _generate_inf_or_operator_handler(b'+', is_delegate=False)


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
    if is_field_name or c in _WHITESPACE or c == ord('/') or c == ord(':'):
        # This might be an annotation or a field name.
        ctx = ctx.derive_pending_symbol(val)
        yield ctx.immediate_transition(ctx.whence)
    else:
        ctx = ctx.derive_ion_type(IonType.SYMBOL)
        yield ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)


@coroutine
def symbol_handler(c, ctx, is_field_name=False):
    in_sexp = ctx.container.ion_type is IonType.SEXP
    if c not in _IDENTIFIER_CHARACTERS:
        if in_sexp and c in _OPERATORS:
            c_next, _ = yield
            ctx.queue.unread(c_next)
            assert ctx.value
            next_ctx = ctx.derive_child_context(ctx.whence)
            next_immediate = next_ctx.immediate_transition(operator_symbol_handler(c, next_ctx))
            yield [ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL, ctx.value)[0]] \
                + [next_immediate[0]], next_ctx
        raise IonException("Illegal character %s in symbol." % (chr(c), ))  # TODO
    val = ctx.value
    val.append(c)
    prev = c
    c, self = yield
    trans = (Transition(None, self), None)
    while True:
        if c not in _WHITESPACE:
            if prev in _WHITESPACE or c in _NUMBER_TERMINATORS or c == ord(':') or (in_sexp and c in _OPERATORS):
                break
            if c not in _IDENTIFIER_CHARACTERS:
                raise IonException("Illegal character %s in symbol" % (chr(c), ))  # TODO
            val.append(c)
        prev = c
        c, _ = yield trans
    if is_field_name or c in _WHITESPACE or c == ord('/') or c == ord(':'):
        # This might be an annotation or a field name.
        ctx = ctx.derive_pending_symbol(val)
        yield ctx.immediate_transition(ctx.whence)
    else:
        ctx = ctx.derive_ion_type(IonType.SYMBOL)
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
        trans = ctx.immediate_transition(symbol_or_keyword_handler(c, ctx, is_field_name=True))
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


_REAL_NUMBER_TABLE = _whitelist({
    ord('d'): decimal_handler,
    ord('e'): float_handler,
    ord('D'): decimal_handler,
    ord('E'): float_handler,
})

_ZERO_START_TABLE = _whitelist({
    ord('0'): timestamp_zero_start_handler,
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
    ord('d'): decimal_handler,
    ord('e'): float_handler,
    ord('D'): decimal_handler,
    ord('E'): float_handler
})

_NUMBER_OR_TIMESTAMP_TABLE = _whitelist({
    ord('-'): timestamp_handler,
    ord('T'): timestamp_handler,
    ord('.'): real_number_handler,
    ord('_'): number_handler,
    ord('d'): decimal_handler,
    ord('e'): float_handler,
    ord('D'): decimal_handler,
    ord('E'): float_handler,
})

_NUMBER_TABLE = _whitelist({
    ord('.'): real_number_handler,
    ord('d'): decimal_handler,
    ord('e'): float_handler,
    ord('D'): decimal_handler,
    ord('E'): float_handler,
})

_NEGATIVE_TABLE = _whitelist({
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
}, negative_inf_or_sexp_hyphen_handler)

_STRUCT_OR_LOB_TABLE = _whitelist({
    ord('{'): lob_handler
}, struct_handler)

_START_TABLE = _whitelist({
    ord('-'): number_negative_start_handler,
    ord('+'): positive_inf_or_sexp_plus_handler,
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
    ord('/'): sexp_slash_handler,  # Comments are handled as a special case, not through _START_TABLE
}, symbol_or_keyword_handler)


@coroutine
def _container_handler(c, ctx):
    _, self = (yield None)
    queue = ctx.queue
    child_context = None
    is_field_name = ctx.ion_type is IonType.STRUCT
    delimiter_required = False
    complete = ctx.depth == 0
    while True:
        if c in ctx.container.end_sequence:
            if child_context and child_context.pending_symbol is not None:
                assert not child_context.value
                yield child_context.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL, child_context.pending_symbol)[0]
            # Yield the close event and go to enclosing container.
            yield Transition(
                IonEvent(IonEventType.CONTAINER_END, ctx.ion_type, depth=ctx.depth-1),
                ctx.whence
            )
        if c in ctx.container.delimiter:
            if child_context and child_context.pending_symbol is not None:
                assert not child_context.value
                yield child_context.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL, child_context.pending_symbol)[0]
                child_context = None
                delimiter_required = True
            if not delimiter_required:
                raise IonException('Encountered delimiter %s without preceding value.' % (chr(ctx.container.delimiter[0])))
            is_field_name = ctx.ion_type is IonType.STRUCT
            delimiter_required = False
            c = None
        if c is not None and c not in _WHITESPACE:
            if c == ord('/'):
                if child_context is None:
                    # TODO duplicated in a branch below
                    # This is the start of a new child value.
                    child_context = ctx.derive_child_context(self)
                if ctx.ion_type is IonType.SEXP:
                    handler = sexp_slash_handler(c, child_context)
                else:
                    handler = comment_handler(c, child_context, self)
            elif delimiter_required:
                # This is not the delimiter, or whitespace, or the start of a comment. Throw.
                raise IonException("Delimiter %s not found after value within %s." % (
                    chr(ctx.container.delimiter[0]), ctx.container.ion_type.name))
            else:
                if child_context and child_context.pending_symbol is not None:
                    # A character besides whitespace, comments, and delimiters has been found, and there is a pending
                    # symbol. That pending symbol is either an annotation, a field name, or a symbol value.
                    if c == ord(':'):
                        if is_field_name:
                            is_field_name = False
                            child_context = child_context.derive_field_name()
                            c = None
                            continue
                        if len(queue) == 0:
                            yield Transition(None, _read_data_handler(self, ctx))
                        peek = queue.read_byte()
                        if peek == ord(':'):
                            child_context = child_context.derive_annotation()
                            c = None  # forces another character to be read safely
                            continue
                        else:
                            raise IonException("Illegal character : in symbol")
                    else:
                        # TODO when is this hit? Investigate. Is it legal?
                        # It's a symbol value
                        yield child_context.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL,
                                                             child_context.pending_symbol)[0]
                        child_context = None
                if child_context is None or (not child_context.annotations and child_context.field_name is None):
                    # This is the start of a new child value.
                    child_context = ctx.derive_child_context(self)
                if is_field_name:
                    handler = field_name_handler(c, child_context)
                else:
                    handler = _START_TABLE[c](c, child_context)  # Initialize the new handler
            container_start = c == ord(b'[') or c == ord(b'(')  # Note: '{' not here because that might be a lob
            read_next = True
            complete = False
            while True:
                if not container_start:
                    if len(queue) == 0:
                        yield Transition(None, _read_data_handler(self, ctx))
                    c = queue.read_byte()
                else:
                    c = None
                    container_start = False
                trans, child_context = handler.send((c, handler))
                next_transition = None
                if not hasattr(trans, 'event'):
                    # Transitions are iterable, so instead of checking to see if the returned value is iterable, we have
                    # to check to see if it's a simple Transition rather than a sequence of Transitions.
                    assert len(trans) == 2
                    next_transition = trans[1]
                    trans = trans[0]
                if trans.event is not None:
                    # This child value is finished. c is now the first character in the next value or sequence.
                    # Hence, a new character should not be read; it should be provided to the handler for the next
                    # child context.
                    yield trans
                    if trans.event.ion_type.is_container and trans.event.event_type is not IonEventType.SCALAR:
                        assert next_transition is None
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
                    complete = ctx.depth == 0
                    delimiter_required = not ((not ctx.container.ion_type) or ctx.container.ion_type is IonType.SEXP)
                    if next_transition is None:
                        break
                    else:
                        trans = next_transition
                elif self is trans.delegate:
                    assert next_transition is None
                    in_comment = isinstance(trans, _CommentTransition)
                    if is_field_name:
                        if c == ord(':') or not (c == ord('/') and in_comment):
                            read_next = False
                            break
                    elif child_context and child_context.pending_symbol is not None:
                        if not in_comment:  # TODO check -- closing slash as above?
                            read_next = False
                            break
                    elif in_comment:
                        # There isn't a pending field name or pending annotations. If this is at the top level,
                        # it may end the stream.
                        complete = ctx.depth == 0
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
            if complete:
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
        ion_type=None,  # Top level
        pending_symbol=None
    )
    return reader_trampoline(_container_handler(None, ctx))
