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
from functools import partial

import collections
import six
import sys

from amazon.ion.core import Transition, ION_STREAM_INCOMPLETE_EVENT, ION_STREAM_END_EVENT, IonType, IonEvent, \
    IonEventType
from amazon.ion.exceptions import IonException
from amazon.ion.reader import BufferQueue, reader_trampoline, ReadEventType, safe_unichr
from amazon.ion.util import record, coroutine, Enum, next_code_point, unicode_iter

_o = six.byte2int
_c = safe_unichr


def _illegal_character(c, ctx, message=''):
    container_type = ctx.container.ion_type is None and 'top-level' or ctx.container.ion_type.name
    value_type = ctx.ion_type is None and 'unknown' or ctx.ion_type.name
    raise IonException('Illegal character %s at position %d in %s value contained in %s. %s Pending value: %s'
                       % (_c(c), ctx.queue.position, value_type, container_type, message, ctx.value))


def _whitelist(dct, fallback=_illegal_character):

    out = defaultdict(lambda: fallback)
    for k, v in six.iteritems(dct):
        out[k] = v
    return out


def _merge_dicts(*args):
    dct = {}
    for arg in args:
        if isinstance(arg, dict):
            merge = arg
        else:
            assert isinstance(arg, tuple)
            keys, value = arg
            merge = dict(zip(keys, [value]*len(keys)))
        dct.update(merge)
    return dct


def _seq(s):
    return tuple(six.iterbytes(s))


_WHITESPACE = _seq(b' \t\n\r')
_VALUE_TERMINATORS = _seq(b'{}[](),\"\' \t\n\r/')
_SYMBOL_TOKEN_TERMINATORS = _WHITESPACE + _seq(b'/:')
_DIGITS = _seq(b'0123456789')
_BINARY_RADIX = _seq(b'Bb')
_BINARY_DIGITS = _seq(b'01')
_HEX_RADIX = _seq(b'Xx')
_HEX_DIGITS = _DIGITS + _seq(b'abcdef')
_DECIMAL_EXPS = _seq(b'Dd')
_FLOAT_EXPS = _seq(b'Ee')
_TIMESTAMP_YEAR_DELIMITERS = _seq(b'-T')
_TIMESTAMP_DELIMITERS = _seq(b'-:+.')
_TIMESTAMP_OFFSET_INDICATORS = _seq(b'Z+-')
_LETTERS = _seq(b'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')
_BASE64_DIGITS = _LETTERS + _DIGITS + _seq(b'+/')
_IDENTIFIER_STARTS = _LETTERS + _seq(b'$_')
_IDENTIFIER_CHARACTERS = _IDENTIFIER_STARTS + _DIGITS
_OPERATORS = _seq(b'!#%&*+\-./;<=>?@^`|~')  # TODO is backslash allowed? Spec: no, java: no, grammar: yes
_COMMON_ESCAPES = _seq(b'abtnfrv?0\'"/\\')  # TODO CR+LF?

_UNDERSCORE = _o(b'_')
_DOT = _o(b'.')
_COMMA = _o(b',')
_COLON = _o(b':')
_SLASH = _o(b'/')
_ASTERISK = _o(b'*')
_BACKSLASH = _o(b'\\')
_NEWLINE = _o(b'\n')
_DOUBLE_QUOTE = _o(b'"')
_SINGLE_QUOTE = _o(b'\'')
_PLUS = _o(b'+')
_MINUS = _o(b'-')
_HYPHEN = _MINUS
_T = _o(b'T')
_Z = _o(b'Z')
_T_LOWER = _o(b't')
_N_LOWER = _o(b'n')
_F_LOWER = _o(b'f')
_ZERO = _DIGITS[0]
_OPEN_BRACE = _o(b'{')
_OPEN_BRACKET = _o(b'[')
_OPEN_PAREN = _o(b'(')
_CLOSE_BRACE = _o(b'}')
_CLOSE_BRACKET = _o(b']')
_CLOSE_PAREN = _o(b')')
_BASE64_PAD = _o(b'=')
_QUESTION_MARK = _o(b'?')
_UNICODE_ESCAPE_2 = _o(b'x')
_UNICODE_ESCAPE_4 = _o(b'u')
_UNICODE_ESCAPE_8 = _o(b'U')

_TRUE_SEQUENCE = _seq(b'rue')
_FALSE_SEQUENCE = _seq(b'alse')
_NAN_SEQUENCE = _seq(b'an')
_INF_SEQUENCE = _seq(b'inf')


class _NullSequence:
    def __init__(self, ion_type, sequence):
        self.ion_type = ion_type
        self.sequence = sequence

    def __getitem__(self, item):
        return self.sequence[item]

_NULL_SEQUENCE = _NullSequence(IonType.NULL, _seq(b'ull'))
_NULL_SYMBOL_SEQUENCE = _NullSequence(IonType.SYMBOL, _seq(b'mbol'))
_NULL_SEXP_SEQUENCE = _NullSequence(IonType.SEXP, _seq(b'xp'))
_NULL_STRING_SEQUENCE = _NullSequence(IonType.STRING, _seq(b'ng'))
_NULL_STRUCT_SEQUENCE = _NullSequence(IonType.STRUCT, _seq(b'ct'))
_NULL_INT_SEQUENCE = _NullSequence(IonType.INT, _seq(b'nt'))
_NULL_FLOAT_SEQUENCE = _NullSequence(IonType.FLOAT, _seq(b'loat'))
_NULL_DECIMAL_SEQUENCE = _NullSequence(IonType.DECIMAL, _seq(b'ecimal'))
_NULL_CLOB_SEQUENCE = _NullSequence(IonType.CLOB, _seq(b'lob'))
_NULL_LIST_SEQUENCE = _NullSequence(IonType.LIST, _seq(b'ist'))
_NULL_BLOB_SEQUENCE = _NullSequence(IonType.BLOB, _seq(b'ob'))
_NULL_BOOL_SEQUENCE = _NullSequence(IonType.BOOL, _seq(b'ol'))
_NULL_TIMESTAMP_SEQUENCE = _NullSequence(IonType.TIMESTAMP, _seq(b'imestamp'))

_NULL_STR_NEXT = {
    _o(b'i'): _NULL_STRING_SEQUENCE,
    _o(b'u'): _NULL_STRUCT_SEQUENCE
}

_NULL_ST_NEXT = {
    _o(b'r'): _NULL_STR_NEXT
}

_NULL_S_NEXT = {
    _o(b'y'): _NULL_SYMBOL_SEQUENCE,
    _o(b'e'): _NULL_SEXP_SEQUENCE,
    _o(b't'): _NULL_ST_NEXT
}

_NULL_B_NEXT = {
    _o(b'l'): _NULL_BLOB_SEQUENCE,
    _o(b'o'): _NULL_BOOL_SEQUENCE
}

_NULL_STARTS = {
    _o(b'n'): _NULL_SEQUENCE,  # null.null
    _o(b's'): _NULL_S_NEXT, # null.string, null.symbol, null.struct, null.sexp
    _o(b'i'): _NULL_INT_SEQUENCE,  # null.int
    _o(b'f'): _NULL_FLOAT_SEQUENCE,  # null.float
    _o(b'd'): _NULL_DECIMAL_SEQUENCE,  # null.decimal
    _o(b'b'): _NULL_B_NEXT,  # null.bool, null.blob
    _o(b'c'): _NULL_CLOB_SEQUENCE,  # null.clob
    _o(b'l'): _NULL_LIST_SEQUENCE,  # null.list
    _o(b't'): _NULL_TIMESTAMP_SEQUENCE,  # null.timestamp
}


class _Container(record(
    'end', 'delimiter', 'ion_type', 'is_delimited'
)):
    """A description of an Ion container.

    Args:
        end (tuple): Singleton containing the container's end character, if any.
        delimiter (tuple): Singleton containing the container's delimiter character, if any.
        ion_type (Optional[IonType]): The container's IonType, if any.
        is_delimited (bool): True if delimiter is not empty; otherwise, False.
    """

_C_TOP_LEVEL = _Container((), (), None, False)
_C_STRUCT = _Container((_CLOSE_BRACE,), (_COMMA,), IonType.STRUCT, True)
_C_LIST = _Container((_CLOSE_BRACKET,), (_COMMA,), IonType.LIST, True)
_C_SEXP = _Container((_CLOSE_PAREN,), (), IonType.SEXP, False)


class CodePointArray(collections.MutableSequence):

    def __init__(self, initial_bytes=None):
        self.value = u''
        if initial_bytes is not None:
            for b in initial_bytes:
                self.append(b)

    def append(self, value):
        self.value += _c(value)

    def __eq__(self, other):
        return other == self.value

    def __ne__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        return len(self.value)

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value

    def insert(self, index, value):
        raise ValueError('Attempted to add code point in middle of sequence')

    def __setitem__(self, index, value):
        raise ValueError('Attempted to set code point in middle of sequence')

    def __getitem__(self, index):
        return self.value[index]

    def __delitem__(self, index):
        raise ValueError('Attempted to delete from code point sequence.')


class _HandlerContext(record(
    'container', 'queue', 'field_name', 'annotations', 'depth', 'whence', 'value', 'ion_type', 'pending_symbol'
)):
    """A context for a handler co-routine.

    Args:
        container (_Container): The description of the container in which this context is contained.
        queue (BufferQueue): The data source for the handler.
        field_name (Optional[SymbolToken]): The token representing the field name for the handled
            value.
        annotations (Optional[Sequence[SymbolToken]]): The sequence of annotations tokens
            for the value to be parsed.
        depth (int): the depth of the parser.
        whence (Coroutine): The reference to the co-routine that this handler should delegate
            back to when the handler is logically done.
        value (bytearray): The (in-progress) value of this context's token.
        ion_type (Optional[IonType]): The IonType of the current token.
        pending_symbol (Optional[bytearray]): A pending symbol, which may end up being an annotation,
            field name, or symbol value.
    """

    def event_transition(self, event_cls, event_type,
                         ion_type=None, value=None, annotations=None, depth=None, whence=None, trans_cls=Transition):
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

        return trans_cls(
            event_cls(event_type, ion_type, value, self.field_name, annotations, depth),
            whence
        ), None

    def immediate_transition(self, delegate, trans_cls=Transition):
        """Returns an immediate transition to another co-routine."""
        return trans_cls(None, delegate), self

    def read_data_event(self, whence, stream_event=ION_STREAM_INCOMPLETE_EVENT):
        """Creates a co-routine for retrieving data.

        Args:
            whence (Coroutine): The co-routine to return to after the data is satisfied.
            stream_event (Optional[IonEvent]): The stream event to return if no bytes are read or
                available.
        """
        return Transition(None, _read_data_handler(whence, self, stream_event=stream_event))

    def next_code_point(self, whence, complete):
        # TODO is it possible to skip the transition when not in quoted text? This would likely lead to improved performance.
        out = _CodePointHolder()
        stream_event = complete and ION_STREAM_END_EVENT or ION_STREAM_INCOMPLETE_EVENT
        return Transition(None, _next_code_point_handler(whence, self, out, stream_event)), out

    def derive_unicode(self):
        if isinstance(self.value, CodePointArray):  # TODO see if this can be prevented
            return self
        return _HandlerContext(
            self.container,
            self.queue,
            self.field_name,
            self.annotations,
            self.depth,
            self.whence,
            CodePointArray(self.value),
            self.ion_type,
            self.pending_symbol
        )

    def derive_container_context(self, ion_type, whence, add_depth=1):
        if ion_type is IonType.STRUCT:
            container = _C_STRUCT
        elif ion_type is IonType.LIST:
            container = _C_LIST
        elif ion_type is IonType.SEXP:
            container = _C_SEXP
        else:
            raise TypeError('Cannot derive container context for non-container type %s.' % (ion_type.name,))
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

    def derive_pending_symbol(self, pending_symbol=None):
        if pending_symbol is None:
            pending_symbol = CodePointArray()
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


def _composite_transition(event, ctx, next_handler, next_ctx=None):
    """Composes an event transition followed by an immediate transition to the handler for the next token.

    This is useful when some lookahead is required to determine if a token has ended, e.g. in the case of long strings.
    """
    if next_ctx is None:
        next_ctx = ctx.derive_child_context(ctx.whence)
    return [event, next_ctx.immediate_transition(next_handler(next_ctx))[0]], next_ctx


class _SelfDelimitingTransition(Transition):
    """Signals that this transition terminates token that is self-delimiting, e.g. quoted string, container, comment."""


@coroutine
def _number_negative_start_handler(c, ctx):
    assert c == _MINUS
    assert len(ctx.value) == 0
    ctx = ctx.derive_ion_type(IonType.INT)
    ctx.value.append(c)
    c, _ = yield
    yield ctx.immediate_transition(_NEGATIVE_TABLE[c](c, ctx))


@coroutine
def _number_zero_start_handler(c, ctx):
    assert c == _ZERO
    assert len(ctx.value) == 0 or (len(ctx.value) == 1 and ctx.value[0] == _MINUS)
    ctx = ctx.derive_ion_type(IonType.INT)
    ctx.value.append(c)
    c, _ = yield
    if c in _VALUE_TERMINATORS:
        trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
        if c == _SLASH:
            trans = ctx.immediate_transition(_number_slash_end_handler(c, ctx, trans))
        yield trans
    yield ctx.immediate_transition(_ZERO_START_TABLE[c](c, ctx))


@coroutine
def _number_or_timestamp_handler(c, ctx):
    assert c in _DIGITS
    ctx = ctx.derive_ion_type(IonType.INT)  # If this is the last digit read, this value is an Int.
    val = ctx.value
    val.append(c)
    c, self = yield
    trans = (Transition(None, self), None)
    while True:
        if c in _VALUE_TERMINATORS:
            trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
            if c == _SLASH:
                trans = ctx.immediate_transition(_number_slash_end_handler(c, ctx, trans))
        else:
            if c not in _DIGITS:
                trans = ctx.immediate_transition(_NUMBER_OR_TIMESTAMP_TABLE[c](c, ctx))
            else:
                val.append(c)
        c, _ = yield trans


@coroutine
def _number_slash_end_handler(c, ctx, event):
    assert c == _SLASH
    c, self = yield
    next_ctx = ctx.derive_child_context(ctx.whence)
    comment = _comment_handler(_SLASH, next_ctx, next_ctx.whence)
    comment.send((c, comment))
    # If the previous line returns without error, it's a valid comment and number int may be emitted.
    yield [event[0], next_ctx.immediate_transition(comment)[0]], next_ctx


def _generate_numeric_handler(charset, transition, assertion, illegal_before_underscore,
                              illegal_at_end=(None,), ion_type=None, append_first_if_not=None):
    @coroutine
    def coefficient_handler(c, ctx):
        assert assertion(c, ctx)
        if ion_type is not None:
            ctx = ctx.derive_ion_type(ion_type)
        val = ctx.value
        if c != append_first_if_not:
            val.append(c)
        prev = c
        c, self = yield
        trans = (Transition(None, self), None)
        while True:
            if c in _VALUE_TERMINATORS:
                if prev == _UNDERSCORE or prev in illegal_at_end:
                    _illegal_character(c, ctx, '%s at end of number.' % (_c(prev),))
                trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
                if c == _SLASH:
                    trans = ctx.immediate_transition(_number_slash_end_handler(c, ctx, trans))
            else:
                if c == _UNDERSCORE:
                    if prev == _UNDERSCORE or prev in illegal_before_underscore:
                        _illegal_character(c, ctx, 'Underscore after %s.' % (_c(prev),))
                else:
                    if c not in charset:
                        trans = transition(prev, c, ctx, trans)
                    else:
                        val.append(c)
            prev = c
            c, _ = yield trans
    return coefficient_handler


def _generate_exponent_handler(ion_type, exp_chars):
    def transition(prev, c, ctx, trans):
        if c == _MINUS and prev in exp_chars:
            ctx.value.append(c)
        else:
            _illegal_character(c, ctx)
        return trans
    illegal = exp_chars + (_MINUS,)
    return _generate_numeric_handler(_DIGITS, transition, lambda c, ctx: c in exp_chars, illegal,
                                     illegal_at_end=illegal, ion_type=ion_type)


def _generate_coefficient_handler(trans_table, assertion=lambda c, ctx: True, ion_type=None, append_first_if_not=None):
    def transition(prev, c, ctx, trans):
        if prev == _UNDERSCORE:
            _illegal_character(c, ctx, 'Underscore before %s.' % (_c(c),))
        return ctx.immediate_transition(trans_table[c](c, ctx))
    return _generate_numeric_handler(_DIGITS, transition, assertion, (_DOT,),
                                     ion_type=ion_type, append_first_if_not=append_first_if_not)


def _generate_radix_int_handler(radix_indicators, charset):
    def assertion(c, ctx):
        return c in radix_indicators and \
               ((len(ctx.value) == 1 and ctx.value[0] == _ZERO) or
                (len(ctx.value) == 2 and ctx.value[0] == _MINUS and ctx.value[1] == _ZERO)) and \
               ctx.ion_type == IonType.INT
    return _generate_numeric_handler(charset, lambda prev, c, ctx: _illegal_character(c, ctx),
                                     assertion, radix_indicators, illegal_at_end=radix_indicators)


_decimal_handler = _generate_exponent_handler(IonType.DECIMAL, _DECIMAL_EXPS)
_float_handler = _generate_exponent_handler(IonType.FLOAT, _FLOAT_EXPS)


_FRACTIONAL_NUMBER_TABLE = _whitelist(
    _merge_dicts(
        (_DECIMAL_EXPS, _decimal_handler),
        (_FLOAT_EXPS, _float_handler)
    )
)

fractional_number_handler = _generate_coefficient_handler(
    _FRACTIONAL_NUMBER_TABLE, assertion=lambda c, ctx: c == _DOT, ion_type=IonType.DECIMAL)

_WHOLE_NUMBER_TABLE = _whitelist(
    _merge_dicts(
        {
            _DOT: fractional_number_handler,
        },
        _FRACTIONAL_NUMBER_TABLE
    )
)

_whole_number_handler = _generate_coefficient_handler(_WHOLE_NUMBER_TABLE, append_first_if_not=_UNDERSCORE)
_binary_int_handler = _generate_radix_int_handler(_BINARY_RADIX, _BINARY_DIGITS)
_hex_int_handler = _generate_radix_int_handler(_HEX_RADIX, _HEX_DIGITS)


@coroutine
def _timestamp_zero_start_handler(c, ctx):
    val = ctx.value
    ctx = ctx.derive_ion_type(IonType.TIMESTAMP)
    if val[0] == _MINUS:
        _illegal_character(c, ctx, 'Negative year not allowed.')
    val.append(c)
    c, self = yield
    trans = (Transition(None, self), None)
    while True:
        if c in _TIMESTAMP_YEAR_DELIMITERS:
            trans = ctx.immediate_transition(_timestamp_handler(c, ctx))
        elif c in _DIGITS:
            val.append(c)
        else:
            _illegal_character(c, ctx)
        c, _ = yield trans


@coroutine
def _timestamp_handler(c, ctx):
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

    assert c in _TIMESTAMP_YEAR_DELIMITERS
    ctx = ctx.derive_ion_type(IonType.TIMESTAMP)
    if len(ctx.value) != 4:  # or should this be left to the parser?
        _illegal_character(c, ctx, 'Timestamp year is %d digits; expected 4.' % (len(ctx.value),))
    val = ctx.value
    # TODO put the components into a Timestamp as strings, so the parser doesn't have to retokenize the components.
    val.append(c)
    prev = c
    c, self = yield
    trans = (Transition(None, self), None)
    state = State.YEAR
    nxt = _DIGITS
    if prev == _T:
        nxt += _VALUE_TERMINATORS
    while True:
        if c not in nxt:
            _illegal_character(c, ctx, 'Expected %r in state %r.' % ([_c(x) for x in nxt], state))
        if c in _VALUE_TERMINATORS:
            trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
            if c == _SLASH:
                trans = ctx.immediate_transition(_number_slash_end_handler(c, ctx, trans))
        else:
            if c == _Z:
                nxt = _VALUE_TERMINATORS
            elif c == _T:
                nxt = _VALUE_TERMINATORS + _DIGITS
            elif c in _TIMESTAMP_DELIMITERS:
                nxt = _DIGITS
            elif c in _DIGITS:
                if prev == _PLUS or (state > State.MONTH and prev == _HYPHEN):
                    state = State.OFF_HOUR
                elif prev in (_TIMESTAMP_DELIMITERS + (_T,)):
                    state = State[state + 1]
                elif prev in _DIGITS:
                    if state == State.MONTH:
                        nxt = _TIMESTAMP_YEAR_DELIMITERS
                    elif state == State.DAY:
                        nxt = (_T,) + _VALUE_TERMINATORS
                    elif state == State.HOUR:
                        nxt = (_COLON,)
                    elif state == State.MINUTE:
                        nxt = _TIMESTAMP_OFFSET_INDICATORS + (_COLON,)
                    elif state == State.SECOND:
                        nxt = _TIMESTAMP_OFFSET_INDICATORS + (_DOT,)
                    elif state == State.FRACTIONAL:
                        nxt = _DIGITS + _TIMESTAMP_OFFSET_INDICATORS + (_DOT,)
                    elif state == State.OFF_HOUR:
                        nxt = (_COLON,) + _VALUE_TERMINATORS
                    elif state == State.OFF_MINUTE:
                        nxt = _VALUE_TERMINATORS
                    else:
                        raise ValueError('Unknown timestamp state %r.' % (state,))
                else:
                    # Reaching this branch would be indicative of a programming error within this state machine.
                    raise ValueError('Digit following %s in timestamp state %r.' % (_c(prev), state))
            val.append(c)
        prev = c
        c, _ = yield trans


@coroutine
def _comment_handler(c, ctx, whence):
    assert c == _SLASH
    c, self = yield
    if c == _SLASH:
        block_comment = False
    elif c == _ASTERISK:
        block_comment = True
    else:
        _illegal_character(c, ctx, 'Illegal character sequence "/%s".' % (_c(c),))
    done = False
    prev = None
    trans = (Transition(None, self), ctx)
    while not done:
        c, _ = yield trans
        if block_comment:
            if prev == _ASTERISK and c == _SLASH:
                done = True
            prev = c
        else:
            if c == _NEWLINE:
                done = True
    yield ctx.immediate_transition(whence, trans_cls=_SelfDelimitingTransition)


@coroutine
def _sexp_slash_handler(c, ctx, whence=None, pending_event=None):
    assert c == _SLASH
    if whence is None:
        whence = ctx.whence
    c, self = yield
    ctx.queue.unread(c)
    if c == _ASTERISK or c == _SLASH:
        yield ctx.immediate_transition(_comment_handler(_SLASH, ctx, whence))
    else:
        if pending_event is not None:
            # Since this is the start of a new value and not a comment, the pending event must be emitted.
            assert pending_event.event is not None
            yield _composite_transition(pending_event, ctx, partial(_operator_symbol_handler, _SLASH))
        yield ctx.immediate_transition(_operator_symbol_handler(_SLASH, ctx))


def _is_escaped(c):
    try:
        return c.is_escaped
    except AttributeError:
        return False


@coroutine
def _long_string_handler(c, ctx, is_field_name=False):
    assert c == _SINGLE_QUOTE
    is_clob = ctx.ion_type is IonType.CLOB
    assert not (is_clob and is_field_name)
    if not is_clob and not is_field_name:
        ctx = ctx.derive_ion_type(IonType.STRING)
    assert not ctx.value
    ctx = ctx.derive_unicode()
    val = ctx.value
    if is_field_name:
        assert not val
        ctx = ctx.derive_pending_symbol()
        val = ctx.pending_symbol
    quotes = 0
    in_data = True
    c, self = yield
    here = (Transition(None, self), None)
    while True:
        trans = here
        # TODO error on disallowed escape sequences
        if c == _SINGLE_QUOTE and not _is_escaped(c):
            quotes += 1
            if quotes == 3:
                in_data = not in_data
                quotes = 0
        else:
            if in_data:
                # Any quotes found in the meantime are part of the data
                val.extend([_SINGLE_QUOTE]*quotes)
                val.append(c)
                quotes = 0
            else:
                if quotes > 0:
                    assert quotes < 3
                    if is_field_name or is_clob:
                        # There are at least two values here, which is illegal for field names or within clobs.
                        _illegal_character(c, ctx, 'Malformed triple-quoted text: %s' % (val,))
                    else:
                        # This string value is followed by a quoted symbol.
                        if ctx.container.is_delimited:
                            _illegal_character(c, ctx, 'Delimiter %s not found after value.'
                                               % (_c(ctx.container.delimiter[0]),))
                        trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
                        if quotes == 1:
                            trans = _composite_transition(trans[0], ctx,
                                                          partial(_quoted_symbol_handler, c, is_field_name=False))
                        else:  # quotes == 2
                            trans = trans[0], ctx.derive_child_context(ctx.whence).derive_pending_symbol()
                elif c not in _WHITESPACE:
                    if is_clob:
                        trans = ctx.immediate_transition(_clob_end_handler(c, ctx))
                    elif c == _SLASH:
                        if ctx.container.ion_type is IonType.SEXP:
                            pending = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)[0]
                            trans = ctx.immediate_transition(_sexp_slash_handler(c, ctx, self, pending))
                        else:
                            trans = ctx.immediate_transition(_comment_handler(c, ctx, self))
                    elif is_field_name:
                        if c != _COLON:
                            _illegal_character(c, ctx, 'Illegal character after field name %s.' % (val,))
                        trans = ctx.immediate_transition(ctx.whence)
                    else:
                        trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value)
        c, _ = yield trans


@coroutine
def _typed_null_handler(c, ctx):
    assert c == _DOT
    c, self = yield
    nxt = _NULL_STARTS
    i = 0
    length = None
    done = False
    trans = (Transition(None, self), None)
    while True:
        if done:
            if c in _VALUE_TERMINATORS or (ctx.container.ion_type is IonType.SEXP and c in _OPERATORS):
                trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, nxt.ion_type, None)
            else:
                _illegal_character(c, ctx, 'Illegal null type.')
        elif length is None:
            if c not in nxt:
                _illegal_character(c, ctx, 'Illegal null type.')
            nxt = nxt[c]
            if isinstance(nxt, _NullSequence):
                length = len(nxt.sequence)
        else:
            if c != nxt[i]:
                _illegal_character(c, ctx, 'Illegal null type.')
            i += 1
            done = i == length
        c, _ = yield trans


@coroutine
def _symbol_or_keyword_handler(c, ctx, is_field_name=False):
    in_sexp = ctx.container.ion_type is IonType.SEXP
    if c not in _IDENTIFIER_STARTS:
        if in_sexp and c in _OPERATORS:
            c_next, _ = yield
            ctx.queue.unread(c_next)
            yield ctx.immediate_transition(_operator_symbol_handler(c, ctx))
        _illegal_character(c, ctx)
    assert not ctx.value
    ctx = ctx.derive_unicode()
    val = ctx.value
    val.append(c)
    maybe_null = c == _N_LOWER
    maybe_nan = maybe_null
    maybe_true = c == _T_LOWER
    maybe_false = c == _F_LOWER
    c, self = yield
    trans = (Transition(None, self), None)
    keyword_trans = None
    match_index = 0
    while True:
        def check_keyword(name, keyword_sequence, ion_type, value, match_transition=lambda: (False, None)):
            maybe_keyword = True
            transition = None
            if match_index < len(keyword_sequence):
                maybe_keyword = c == keyword_sequence[match_index]
            else:
                transitioned, transition = match_transition()
                if transitioned:
                    pass
                elif c in _VALUE_TERMINATORS:
                    if is_field_name:
                        _illegal_character(c, ctx, '%s keyword as field name not allowed.' % (name,))
                    transition = ctx.event_transition(IonEvent, IonEventType.SCALAR, ion_type, value)
                elif c == _COLON:
                    message = ''
                    if is_field_name:
                        message = '%s keyword as field name not allowed.' % (name,)
                    _illegal_character(c, ctx, message)
                elif in_sexp and c in _OPERATORS:
                    transition = ctx.event_transition(IonEvent, IonEventType.SCALAR, ion_type, value)
                else:
                    maybe_keyword = False
            return maybe_keyword, transition
        if maybe_null:
            def check_null_dot():
                transition = None
                found = c == _DOT
                if found:
                    if is_field_name:
                        _illegal_character(c, ctx, "Illegal character in field name.")
                    transition = ctx.immediate_transition(_typed_null_handler(c, ctx))
                return found, transition
            maybe_null, keyword_trans = check_keyword('null', _NULL_SEQUENCE.sequence, IonType.NULL, None, check_null_dot)
        if maybe_nan:
            maybe_nan, keyword_trans = check_keyword('nan', _NAN_SEQUENCE, IonType.FLOAT, bytearray(b'nan'))  # TODO clunky; avoids the unicode
        elif maybe_true:
            maybe_true, keyword_trans = check_keyword('true', _TRUE_SEQUENCE, IonType.BOOL, True)
        elif maybe_false:
            maybe_false, keyword_trans = check_keyword('false', _FALSE_SEQUENCE, IonType.BOOL, False)
        if maybe_null or maybe_nan or maybe_true or maybe_false:
            if keyword_trans is not None:
                trans = keyword_trans
            else:
                val.append(c)
                match_index += 1
        else:
            if c in _SYMBOL_TOKEN_TERMINATORS:
                # This might be an annotation or a field name
                ctx = ctx.derive_pending_symbol(val)
                trans = ctx.immediate_transition(ctx.whence)
            elif c in _VALUE_TERMINATORS or (in_sexp and c in _OPERATORS):
                trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL, val)
            else:
                trans = ctx.immediate_transition(_identifier_symbol_handler(c, ctx, is_field_name=is_field_name))
        c, _ = yield trans


def _generate_inf_or_operator_handler(c_start, is_delegate=True):
    @coroutine
    def inf_or_operator_handler(c, ctx):
        next_ctx = None
        if not is_delegate:
            ctx.value.append(c_start)
            c, self = yield
        else:
            assert ctx.value[0] == c_start
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
                    if c in _VALUE_TERMINATORS or (ctx.container.ion_type is IonType.SEXP and c in _OPERATORS):
                        yield ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.FLOAT, bytearray(six.int2byte(c_start) + b'inf'))
                    else:
                        maybe_inf = False
            if maybe_inf:
                match_index += 1
            else:
                ctx = ctx.derive_unicode()
                if match_index > 0:
                    next_ctx = ctx.derive_child_context(ctx.whence)
                    for ch in _INF_SEQUENCE[0:match_index]:
                        next_ctx.value.append(ch)
                break
            c, self = yield trans
        if ctx.container is not _C_SEXP:
            _illegal_character(c, next_ctx is None and ctx or next_ctx)
        if match_index == 0:
            if c in _OPERATORS:
                yield ctx.immediate_transition(_operator_symbol_handler(c, ctx))
            yield ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL, ctx.value)
        yield _composite_transition(ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL, ctx.value)[0],
                                    ctx, partial(_identifier_symbol_handler, c), next_ctx)
    return inf_or_operator_handler


_negative_inf_or_sexp_hyphen_handler = _generate_inf_or_operator_handler(_MINUS)
_positive_inf_or_sexp_plus_handler = _generate_inf_or_operator_handler(_PLUS, is_delegate=False)


@coroutine
def _operator_symbol_handler(c, ctx):
    assert c in _OPERATORS
    ctx = ctx.derive_unicode()
    val = ctx.value
    val.append(c)
    c, self = yield
    trans = (Transition(None, self), None)
    while c in _OPERATORS:
        val.append(c)
        c, _ = yield trans
    yield ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL, val)


def _symbol_token_end(c, ctx, is_field_name):
    if is_field_name or c in _SYMBOL_TOKEN_TERMINATORS:
        # This might be an annotation or a field name.
        ctx = ctx.derive_pending_symbol(ctx.value)
        trans = ctx.immediate_transition(ctx.whence)
    else:
        trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL, ctx.value)
    return trans


@coroutine
def _identifier_symbol_handler(c, ctx, is_field_name=False):
    in_sexp = ctx.container.ion_type is IonType.SEXP
    ctx = ctx.derive_unicode()
    if c not in _IDENTIFIER_CHARACTERS:
        if in_sexp and c in _OPERATORS:
            c_next, _ = yield
            ctx.queue.unread(c_next)
            assert ctx.value
            yield _composite_transition(
                ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL, ctx.value)[0],
                ctx,
                partial(_operator_symbol_handler, c)
            )
        _illegal_character(c, ctx.derive_ion_type(IonType.SYMBOL))
    val = ctx.value
    val.append(c)
    prev = c
    c, self = yield
    trans = (Transition(None, self), None)
    while True:
        if c not in _WHITESPACE:
            if prev in _WHITESPACE or c in _VALUE_TERMINATORS or c == _COLON or (in_sexp and c in _OPERATORS):
                break
            if c not in _IDENTIFIER_CHARACTERS:
                _illegal_character(c, ctx.derive_ion_type(IonType.SYMBOL))
            val.append(c)
        prev = c
        c, _ = yield trans
    yield _symbol_token_end(c, ctx, is_field_name)


def _generate_quoted_text_handler(delimiter, assertion, after, append_first=True,
                                  before=lambda ctx, is_field_name: (ctx, ctx.value, False),
                                  on_close=lambda ctx: None):
    @coroutine
    def quoted_text_handler(c, ctx, is_field_name=False):
        assert assertion(c)
        ctx = ctx.derive_unicode()
        ctx, val, event_on_close = before(ctx, is_field_name)
        if append_first:
            val.append(c)
        c, self = yield
        trans = (Transition(None, self), None)
        done = False
        while not done:
            # TODO error on disallowed escape sequences
            if c == delimiter and not _is_escaped(c):
                done = True
                if event_on_close:
                    trans = on_close(ctx)
            else:
                # TODO should a backslash be appended?
                val.append(c)
            c, _ = yield trans
        yield after(c, ctx, is_field_name)
    return quoted_text_handler


def _generate_short_string_handler():
    def before(ctx, is_field_name):
        is_clob = ctx.ion_type is IonType.CLOB
        assert not (is_clob and is_field_name)
        is_string = not is_clob and not is_field_name
        if is_string:
            ctx = ctx.derive_ion_type(IonType.STRING)
        val = ctx.value
        if is_field_name:
            assert not val
            ctx = ctx.derive_pending_symbol()
            val = ctx.pending_symbol
        return ctx, val, is_string

    def on_close(ctx):
        return ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value,
                                    trans_cls=_SelfDelimitingTransition)

    def after(c, ctx, is_field_name):
        return ctx.immediate_transition(is_field_name and ctx.whence or _clob_end_handler(c, ctx))

    return _generate_quoted_text_handler(_DOUBLE_QUOTE, lambda c: c == _DOUBLE_QUOTE, after, append_first=False,
                                         before=before, on_close=on_close)


_short_string_handler = _generate_short_string_handler()
_quoted_symbol_handler = _generate_quoted_text_handler(_SINGLE_QUOTE, lambda c: (c != _SINGLE_QUOTE or _is_escaped(c)), _symbol_token_end)


def _generate_single_quote_handler(on_single_quote, on_other):
    @coroutine
    def single_quote_handler(c, ctx, is_field_name=False):
        assert c == _SINGLE_QUOTE
        c, self = yield
        if c == _SINGLE_QUOTE and not _is_escaped(c):
            yield ctx.immediate_transition(on_single_quote(c, ctx, is_field_name))
        else:
            yield on_other(c, ctx, is_field_name)
    return single_quote_handler


_two_single_quotes_handler = _generate_single_quote_handler(
    _long_string_handler,
    lambda c, ctx, is_field_name: ctx.derive_pending_symbol().immediate_transition(ctx.whence)  # Empty symbol.
)
_string_or_symbol_handler = _generate_single_quote_handler(
    _two_single_quotes_handler,
    lambda c, ctx, is_field_name: ctx.immediate_transition(_quoted_symbol_handler(c, ctx, is_field_name))
)


@coroutine
def _struct_or_lob_handler(c, ctx):
    assert c == _OPEN_BRACE
    c, self = yield
    yield ctx.immediate_transition(_STRUCT_OR_LOB_TABLE[c](c, ctx))


@coroutine
def _lob_start_handler(c, ctx):
    assert c == _OPEN_BRACE
    c, self = yield
    trans = (Transition(None, self), None)
    quotes = 0
    while True:
        if c in _WHITESPACE:
            if quotes > 0:
                _illegal_character(c, ctx)
        elif c == _DOUBLE_QUOTE:
            ctx = ctx.derive_ion_type(IonType.CLOB)
            if quotes > 0:
                _illegal_character(c, ctx)
            yield ctx.immediate_transition(_short_string_handler(c, ctx))
        elif c == _SINGLE_QUOTE:
            if not quotes:
                ctx = ctx.derive_ion_type(IonType.CLOB)
            quotes += 1
            if quotes == 3:
                yield ctx.immediate_transition(_long_string_handler(c, ctx))
        else:
            yield ctx.immediate_transition(_blob_end_handler(c, ctx))
        c, _ = yield trans


def _generate_lob_end_handler(ion_type, action, validate=lambda c, ctx, action_res: None):
    assert ion_type is IonType.BLOB or ion_type is IonType.CLOB

    @coroutine
    def lob_end_handler(c, ctx):
        val = ctx.value
        prev = c
        action_res = None
        if c != _CLOSE_BRACE and c not in _WHITESPACE:
            action_res = action(c, ctx, prev, action_res)
        c, self = yield
        trans = (Transition(None, self), None)
        while True:
            if c in _WHITESPACE:
                if prev == _CLOSE_BRACE:
                    _illegal_character(c, ctx.derive_ion_type(ion_type), 'Expected }.')
            elif c == _CLOSE_BRACE:
                if prev == _CLOSE_BRACE:
                    validate(c, ctx, action_res)
                    break
            else:
                action_res = action(c, ctx, prev, action_res)
            prev = c
            c, _ = yield trans
        yield ctx.event_transition(IonEvent, IonEventType.SCALAR, ion_type, val, trans_cls=_SelfDelimitingTransition)
    return lob_end_handler


def _generate_blob_end_handler():
    def expand_res(res):
        if res is None:
            return 0, 0
        return res

    def action(c, ctx, prev, res):
        num_digits, num_pads = expand_res(res)
        if c in _BASE64_DIGITS:
            if prev == _CLOSE_BRACE or prev == _BASE64_PAD:
                _illegal_character(c, ctx.derive_ion_type(IonType.BLOB))
            num_digits += 1
        elif c == _BASE64_PAD:
            if prev == _CLOSE_BRACE:
                _illegal_character(c, ctx.derive_ion_type(IonType.BLOB))
            num_pads += 1
        else:
            _illegal_character(c, ctx.derive_ion_type(IonType.BLOB))
        ctx.value.append(c)
        return num_digits, num_pads

    def validate(c, ctx, res):
        num_digits, num_pads = expand_res(res)
        if num_pads > 3 or (num_digits + num_pads) % 4 != 0:
            _illegal_character(c, ctx, 'Incorrect number of pad characters (%d) for a blob of %d base-64 digits.'
                               % (num_pads, num_digits))

    return _generate_lob_end_handler(IonType.BLOB, action, validate)

_blob_end_handler = _generate_blob_end_handler()
_clob_end_handler = _generate_lob_end_handler(IonType.CLOB, lambda c, ctx, prev, action_res: _illegal_character(c, ctx))


_single_quoted_field_name_handler = partial(_string_or_symbol_handler, is_field_name=True)
_double_quoted_field_name_handler = partial(_short_string_handler, is_field_name=True)
_unquoted_field_name_handler = partial(_symbol_or_keyword_handler, is_field_name=True)


def _generate_container_start_handler(ion_type, before_yield=lambda c, ctx: None):
    assert ion_type.is_container

    @coroutine
    def container_start_handler(c, ctx):
        before_yield(c, ctx)
        yield
        yield ctx.event_transition(IonEvent, IonEventType.CONTAINER_START, ion_type)
    return container_start_handler


# Struct requires unread_byte because we had to read one char past the { to make sure it wasn't a lob.
_struct_handler = _generate_container_start_handler(IonType.STRUCT, lambda c, ctx: ctx.queue.unread(c))
_list_handler = _generate_container_start_handler(IonType.LIST)
_sexp_handler = _generate_container_start_handler(IonType.SEXP)


@coroutine
def _read_data_handler(whence, ctx, stream_event=ION_STREAM_INCOMPLETE_EVENT):
    """Creates a co-routine for retrieving data up to a requested size.

    Args:
        whence (Coroutine): The co-routine to return to after the data is satisfied.
        ctx (_HandlerContext): The context for the read.
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
                queue.extend(data)
                yield Transition(None, whence)
        trans = Transition(stream_event, self)


_ZERO_START_TABLE = _whitelist(
    _merge_dicts(
        _WHOLE_NUMBER_TABLE,
        (_DIGITS, _timestamp_zero_start_handler),
        (_BINARY_RADIX, _binary_int_handler),
        (_HEX_RADIX, _hex_int_handler)
    )
)

_NUMBER_OR_TIMESTAMP_TABLE = _whitelist(
    _merge_dicts(
        {
            _UNDERSCORE: _whole_number_handler,
        },
        _WHOLE_NUMBER_TABLE,
        (_TIMESTAMP_YEAR_DELIMITERS, _timestamp_handler)
    )
)

_NEGATIVE_TABLE = _whitelist(
    _merge_dicts(
        {
            _ZERO: _number_zero_start_handler,
        },
        (_DIGITS[1:], _whole_number_handler)
    ),
    fallback=_negative_inf_or_sexp_hyphen_handler
)

_STRUCT_OR_LOB_TABLE = _whitelist({
    _OPEN_BRACE: _lob_start_handler
}, _struct_handler)


_FIELD_NAME_START_TABLE = _whitelist(
    _merge_dicts(
        {
            _SINGLE_QUOTE: _single_quoted_field_name_handler,
            _DOUBLE_QUOTE: _double_quoted_field_name_handler,
        },
        (_IDENTIFIER_STARTS, _unquoted_field_name_handler)
    ),
    fallback=partial(_illegal_character, message='Illegal character in field name.')
)

_VALUE_START_TABLE = _whitelist(
    _merge_dicts(
        {
            _MINUS: _number_negative_start_handler,
            _PLUS: _positive_inf_or_sexp_plus_handler,
            _ZERO: _number_zero_start_handler,
            _OPEN_BRACE: _struct_or_lob_handler,
            _OPEN_PAREN: _sexp_handler,
            _OPEN_BRACKET: _list_handler,
            _SINGLE_QUOTE: _string_or_symbol_handler,
            _DOUBLE_QUOTE: _short_string_handler,
        },
        (_DIGITS[1:], _number_or_timestamp_handler)
    ),
    fallback=_symbol_or_keyword_handler
)


@coroutine
def _container_handler(c, ctx):
    _, self = (yield None)
    queue = ctx.queue
    child_context = None
    is_field_name = ctx.ion_type is IonType.STRUCT
    delimiter_required = False
    complete = ctx.depth == 0

    def has_pending_symbol():
        return child_context and child_context.pending_symbol is not None

    def symbol_value_event():
        return child_context.event_transition(
            IonEvent, IonEventType.SCALAR, IonType.SYMBOL, child_context.pending_symbol)[0]

    def pending_symbol_value():
        if has_pending_symbol():
            assert not child_context.value
            if ctx.ion_type is IonType.STRUCT and child_context.field_name is None:
                _illegal_character(c, ctx,
                                   'Encountered STRUCT value %s without field name.' % (child_context.pending_symbol,))
            return symbol_value_event()
        return None

    def is_value_decorated():
        return child_context is not None and (child_context.annotations or child_context.field_name is not None)

    while True:
        # Loop over all values in this container.
        if c in ctx.container.end or c in ctx.container.delimiter:
            symbol_event = pending_symbol_value()
            if symbol_event is not None:
                yield symbol_event
                child_context = None
                delimiter_required = True
            if c in ctx.container.end:
                if not delimiter_required and is_value_decorated():
                    _illegal_character(c, child_context,
                                       'Dangling field name (%s) and/or annotation(s) (%r) at end of container.'
                                       % (child_context.field_name, child_context.annotations))
                # Yield the close event and go to enclosing container. This coroutine instance will never resume.
                yield Transition(
                    IonEvent(IonEventType.CONTAINER_END, ctx.ion_type, depth=ctx.depth-1),
                    ctx.whence
                )
                raise ValueError('Resumed a finished container handler.')
            else:
                if not delimiter_required:
                    _illegal_character(c, ctx.derive_child_context(None),
                                       'Encountered delimiter %s without preceding value.'
                                       % (_c(ctx.container.delimiter[0]),))
                is_field_name = ctx.ion_type is IonType.STRUCT
                delimiter_required = False
                c = None
        if c is not None and c not in _WHITESPACE:
            if c == _SLASH:
                if child_context is None:
                    # This is the start of a new child value (or, if this is a comment, a new value will start after the
                    # comment ends).
                    child_context = ctx.derive_child_context(self)
                if ctx.ion_type is IonType.SEXP:
                    handler = _sexp_slash_handler(c, child_context, pending_event=pending_symbol_value())
                else:
                    handler = _comment_handler(c, child_context, self)
            elif delimiter_required:
                # This is not the delimiter, or whitespace, or the start of a comment. Throw.
                _illegal_character(c, ctx.derive_child_context(None), 'Delimiter %s not found after value.'
                                   % (_c(ctx.container.delimiter[0]),))
            elif has_pending_symbol():
                # A character besides whitespace, comments, and delimiters has been found, and there is a pending
                # symbol. That pending symbol is either an annotation, a field name, or a symbol value.
                if c == _COLON:
                    if is_field_name:
                        is_field_name = False
                        child_context = child_context.derive_field_name()
                        c = None
                    else:
                        cp_trans, cp = ctx.next_code_point(self, complete)
                        yield cp_trans
                        c = cp.code_point
                        if c == _COLON:
                            child_context = child_context.derive_annotation()
                            c = None  # forces another character to be read safely
                        else:
                            # Colon that doesn't indicate a field name or annotation.
                            _illegal_character(c, child_context)
                else:
                    if is_field_name:
                        _illegal_character(c, child_context, 'Illegal character after field name %s.'
                                           % (child_context.pending_symbol))
                    # It's a symbol value delimited by something other than a comma (i.e. whitespace or comment)
                    yield symbol_value_event()
                    child_context = None
                    delimiter_required = ctx.container.is_delimited
                continue
            else:
                if not is_value_decorated():
                    # This is the start of a new child value.
                    child_context = ctx.derive_child_context(self)
                if is_field_name:
                    handler = _FIELD_NAME_START_TABLE[c](c, child_context)
                else:
                    handler = _VALUE_START_TABLE[c](c, child_context)  # Initialize the new handler
            container_start = c == _OPEN_BRACKET or c == _OPEN_PAREN  # Note: '{' not here because that might be a lob
            read_next = True
            complete = False
            while True:
                # Loop over all characters in the current token. A token is either a non-symbol value or a pending
                # symbol, which may end up being a field name, annotation, or symbol value.
                if container_start:
                    c = None
                    container_start = False
                else:
                    cp_trans, cp = ctx.next_code_point(self, complete)
                    yield cp_trans
                    c = cp.code_point
                trans, child_context = handler.send((c, handler))
                next_transition = None
                if not hasattr(trans, 'event'):
                    # This is a composite transition, i.e. it contains an event transition followed by an immediate
                    # transition to the handler coroutine for the next token.
                    assert len(trans) == 2
                    trans, next_transition = trans
                    assert trans.event is not None
                    assert next_transition.event is None
                if trans.event is not None:
                    # This child value is finished. c is now the first character in the next value or sequence.
                    # Hence, a new character should not be read; it should be provided to the handler for the next
                    # child context.
                    yield trans
                    end_container = trans.event.ion_type.is_container and trans.event.event_type is not IonEventType.SCALAR
                    if end_container:
                        assert next_transition is None
                        yield Transition(
                            None,
                            _container_handler(c, ctx.derive_container_context(trans.event.ion_type, self))
                        )
                    complete = ctx.depth == 0
                    if end_container or isinstance(trans, _SelfDelimitingTransition):
                        # The end of the value has been reached, and c needs to be updated
                        cp_trans, cp = ctx.next_code_point(self, complete)
                        yield cp_trans
                        c = cp.code_point
                    read_next = False
                    delimiter_required = ctx.container.is_delimited
                    if next_transition is None:
                        break
                    else:
                        trans = next_transition
                elif self is trans.delegate:
                    assert next_transition is None
                    in_comment = isinstance(trans, _SelfDelimitingTransition)
                    if is_field_name:
                        if c == _COLON or not (c == _SLASH and in_comment):
                            read_next = False
                            break
                    elif has_pending_symbol():
                        if not in_comment:
                            read_next = False
                            break
                    elif in_comment:
                        # There isn't a pending field name or pending annotations. If this is at the top level,
                        # it may end the stream.
                        complete = ctx.depth == 0
                    # This happens at the end of a comment within this container, or when a symbol token has been
                    # found. In both cases, an event should not be emitted. Read the next character and continue.
                    cp_trans, cp = ctx.next_code_point(self, complete)
                    yield cp_trans
                    c = cp.code_point
                    read_next = False
                    break
                # This is an immediate transition to a handler (may be the same one) for the current token.
                handler = trans.delegate
            if read_next and len(queue) == 0:
                # This will cause the next loop to fall through to the else branch below to ask for more input.
                c = None
        else:
            cp_trans, cp = ctx.next_code_point(self, complete)
            yield cp_trans
            c = cp.code_point


@coroutine
def _skip_trampoline(handler):
    """Intercepts events from container handlers, emitting them only if they should not be skipped."""
    data_event, self = (yield None)
    delegate = handler
    event = None
    depth = 0
    while True:
        def pass_through():
            _trans = delegate.send(Transition(data_event, delegate))
            return _trans, _trans.delegate, _trans.event

        if data_event is not None and data_event.type is ReadEventType.SKIP:
            while True:
                trans, delegate, event = pass_through()
                if event is not None:
                    if event.event_type is IonEventType.CONTAINER_END and event.depth <= depth:
                        break
                if event is None or event.event_type is IonEventType.INCOMPLETE:
                    data_event, _ = yield Transition(event, self)
        else:
            trans, delegate, event = pass_through()
            if event is not None and (event.event_type is IonEventType.CONTAINER_START or
                                      event.event_type is IonEventType.CONTAINER_END):
                depth = event.depth
        data_event, _ = yield Transition(event, self)


class CodePoint(int):
    def __init__(self, *args, **kwargs):
        self.char = None
        self.is_escaped = False


class _CodePointHolder:
    def __init__(self):
        self.code_point = None


@coroutine
def _next_code_point_handler(whence, ctx, out, stream_event):
    data_event, self = yield
    queue = ctx.queue
    if len(queue) == 0:
        yield ctx.read_data_event(self, stream_event=stream_event)
    queue_iter = iter(queue)
    code_point_generator = next_code_point(queue, queue_iter, yield_char=True)
    cp_pair = next(code_point_generator)
    code_point, surrogates = (None, ())
    if cp_pair is not None:
        code_point, surrogates = cp_pair
    if code_point == _BACKSLASH:  # TODO and escapes allowed
        escape_sequence = b'' + six.int2byte(_BACKSLASH)
        num_digits = None
        while True:
            if len(queue) == 0:
                yield ctx.read_data_event(self, ION_STREAM_INCOMPLETE_EVENT)
            code_point = next(queue_iter)
            if six.indexbytes(escape_sequence, -1) == _BACKSLASH:
                if code_point == _o(b'x'):
                    num_digits = 4  # 2-digit hex escapes
                elif code_point == _o(b'u'):
                    num_digits = 6  # 4-digit unicode escapes
                elif code_point == _o(b'U'):
                    num_digits = 10  # 8-digit unicode escapes
                elif code_point in _COMMON_ESCAPES:
                    if code_point == _SLASH or code_point == _QUESTION_MARK:
                        escape_sequence = b''  # Drop the \. Python does not recognize these as escapes.
                    escape_sequence += six.int2byte(code_point)
                    break
                else:
                    # This is a backslash followed by an invalid escape character. This is illegal.
                    _illegal_character(code_point, ctx, 'Invalid escape sequence \\%s.' % (_c(code_point),))
                escape_sequence += six.int2byte(code_point)
            else:
                escape_sequence += six.int2byte(code_point)
                if len(escape_sequence) == num_digits:
                    break
        escape_sequence = escape_sequence.decode('unicode-escape')
        cp_iter = unicode_iter(escape_sequence)
        out.code_point = CodePoint(next(cp_iter))
        out.code_point.char = escape_sequence
        out.code_point.is_escaped = True
        yield Transition(None, whence)
    while code_point is None:
        yield ctx.read_data_event(self)
        cp_pair = next(code_point_generator)
        if cp_pair is not None:
            code_point, surrogates = cp_pair
    if len(surrogates) > 1:
        out.code_point = CodePoint(code_point)
        out.code_point.char = u''
        for surrogate in surrogates:
            out.code_point.char += six.unichr(surrogate)
    else:
        out.code_point = code_point
    yield Transition(None, whence)


def reader(queue=None, is_unicode=False):
    """Returns a raw binary reader co-routine.

    Args:
        queue (Optional[BufferQueue]): The buffer read data for parsing, if ``None`` a
            new one will be created.

        is_unicode (Optional[bool]): True if all input data to this reader will be of unicode text type; False if all
            input data to this reader will be of binary type.

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
        queue = BufferQueue(is_unicode)
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
    return reader_trampoline(_skip_trampoline(_container_handler(None, ctx)))
