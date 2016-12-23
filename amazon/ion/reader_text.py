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

import base64
from decimal import Decimal
from collections import defaultdict
from functools import partial

import six

from amazon.ion.core import Transition, ION_STREAM_INCOMPLETE_EVENT, ION_STREAM_END_EVENT, IonType, IonEvent, \
    IonEventType, IonThunkEvent, TimestampPrecision, timestamp, ION_VERSION_MARKER_EVENT
from amazon.ion.exceptions import IonException
from amazon.ion.reader import BufferQueue, reader_trampoline, ReadEventType, CodePointArray
from amazon.ion.symbols import SymbolToken, TEXT_ION_1_0
from amazon.ion.util import record, coroutine, Enum, _next_code_point, unicode_iter, CodePoint, UCS2, safe_unichr

_ord = six.byte2int
_chr = safe_unichr


def _illegal_character(c, ctx, message=''):
    """Raises an IonException upon encountering the given illegal character in the given context.

    Args:
        c (int): Ordinal of the illegal character.
        ctx (_HandlerContext):  Context in which the illegal character was encountered.
        message (Optional[str]): Additional information, as necessary.

    """
    container_type = ctx.container.ion_type is None and 'top-level' or ctx.container.ion_type.name
    value_type = ctx.ion_type is None and 'unknown' or ctx.ion_type.name
    c = 'EOF' if BufferQueue.is_eof(c) else _chr(c)
    raise IonException('Illegal character %s at position %d in %s value contained in %s. %s Pending value: %s'
                       % (c, ctx.queue.position, value_type, container_type, message, ctx.value))


def _defaultdict(dct, fallback=_illegal_character):
    """Wraps the given dictionary such that the given fallback function will be called when a nonexistent key is
    accessed.
    """
    out = defaultdict(lambda: fallback)
    for k, v in six.iteritems(dct):
        out[k] = v
    return out


def _merge_mappings(*args):
    """Merges a sequence of dictionaries and/or tuples into a single dictionary.

    If a given argument is a tuple, it must have two elements, the first of which is a sequence of keys and the second
    of which is a single value, which will be mapped to from each of the keys in the sequence.
    """
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
    """Converts bytes to a sequence of integer code points."""
    return tuple(six.iterbytes(s))


_ENCODING = 'utf-8'

# NOTE: the following are stored as sequences of integer code points. This simplifies dealing with inconsistencies
# between how bytes objects are handled in python 2 and 3, and simplifies logic around comparing multi-byte characters.
_WHITESPACE_NOT_NL = _seq(b' \t\v\f')
_WHITESPACE = _WHITESPACE_NOT_NL + _seq(b'\n\r')
_VALUE_TERMINATORS = _seq(b'{}[](),\"\' \t\n\r/')
_SYMBOL_TOKEN_TERMINATORS = _WHITESPACE + _seq(b'/:')
_DIGITS = _seq(b'0123456789')
_BINARY_RADIX = _seq(b'Bb')
_BINARY_DIGITS = _seq(b'01')
_HEX_RADIX = _seq(b'Xx')
_HEX_DIGITS = _DIGITS + _seq(b'abcdefABCDEF')
_DECIMAL_EXPS = _seq(b'Dd')
_FLOAT_EXPS = _seq(b'Ee')
_SIGN = _seq(b'+-')
_TIMESTAMP_YEAR_DELIMITERS = _seq(b'-T')
_TIMESTAMP_DELIMITERS = _seq(b'-:+.')
_TIMESTAMP_OFFSET_INDICATORS = _seq(b'Z+-')
_LETTERS = _seq(b'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')
_BASE64_DIGITS = _LETTERS + _DIGITS + _seq(b'+/')
_IDENTIFIER_STARTS = _LETTERS + _seq(b'_')  # Note: '$' is dealt with separately.
_IDENTIFIER_CHARACTERS = _IDENTIFIER_STARTS + _DIGITS + _seq(b'$')
_OPERATORS = _seq(b'!#%&*+-./;<=>?@^`|~')
_COMMON_ESCAPES = _seq(b'abtnfrv?0\'"/\\')
_NEWLINES = _seq(b'\r\n')

_UNDERSCORE = _ord(b'_')
_DOT = _ord(b'.')
_COMMA = _ord(b',')
_COLON = _ord(b':')
_SLASH = _ord(b'/')
_ASTERISK = _ord(b'*')
_BACKSLASH = _ord(b'\\')
_CARRIAGE_RETURN = _ord(b'\r')
_NEWLINE = _ord(b'\n')
_DOUBLE_QUOTE = _ord(b'"')
_SINGLE_QUOTE = _ord(b'\'')
_DOLLAR_SIGN = _ord(b'$')
_PLUS = _ord(b'+')
_MINUS = _ord(b'-')
_HYPHEN = _MINUS
_T = _ord(b'T')
_Z = _ord(b'Z')
_T_LOWER = _ord(b't')
_N_LOWER = _ord(b'n')
_F_LOWER = _ord(b'f')
_ZERO = _DIGITS[0]
_OPEN_BRACE = _ord(b'{')
_OPEN_BRACKET = _ord(b'[')
_OPEN_PAREN = _ord(b'(')
_CLOSE_BRACE = _ord(b'}')
_CLOSE_BRACKET = _ord(b']')
_CLOSE_PAREN = _ord(b')')
_BASE64_PAD = _ord(b'=')
_QUESTION_MARK = _ord(b'?')
_UNICODE_ESCAPE_2 = _ord(b'x')
_UNICODE_ESCAPE_4 = _ord(b'u')
_UNICODE_ESCAPE_8 = _ord(b'U')

_MAX_TEXT_CHAR = 0x10ffff
_MAX_CLOB_CHAR = 0x7e
_MIN_QUOTED_CHAR = 0x20

# The following suffixes are used for comparison when a token is found that starts with the first letter in
# the keyword. For example, when a new token starts with 't', the next three characters must match those in
# _TRUE_SUFFIX, followed by an acceptable termination character, in order for the token to match the 'true' keyword.
_TRUE_SUFFIX = _seq(b'rue')
_FALSE_SUFFIX = _seq(b'alse')
_NAN_SUFFIX = _seq(b'an')
_INF_SUFFIX = _seq(b'inf')
_IVM_SUFFIX = _seq(TEXT_ION_1_0.encode(_ENCODING))

_IVM_TOKEN = SymbolToken(TEXT_ION_1_0, sid=None)

_POS_INF = float('+inf')
_NEG_INF = float('-inf')
_NAN = float('nan')


def _ends_value(c):
    return c in _VALUE_TERMINATORS or BufferQueue.is_eof(c)


class _NullSequence:
    """Contains the terminal character sequence for the typed null suffix of the given IonType, starting with the first
    character after the one which disambiguated the type.

    For example, SYMBOL's _NullSequence contains the characters 'mbol' because 'null.s' is ambiguous until 'y' is found,
    at which point it must end in 'mbol'.

    Instances are used as leaves of the typed null prefix tree below.
    """
    def __init__(self, ion_type, sequence):
        self.ion_type = ion_type
        self.sequence = sequence

    def __getitem__(self, item):
        return self.sequence[item]

_NULL_SUFFIX = _NullSequence(IonType.NULL, _seq(b'ull'))
_NULL_SYMBOL_SUFFIX = _NullSequence(IonType.SYMBOL, _seq(b'mbol'))
_NULL_SEXP_SUFFIX = _NullSequence(IonType.SEXP, _seq(b'xp'))
_NULL_STRING_SUFFIX = _NullSequence(IonType.STRING, _seq(b'ng'))
_NULL_STRUCT_SUFFIX = _NullSequence(IonType.STRUCT, _seq(b'ct'))
_NULL_INT_SUFFIX = _NullSequence(IonType.INT, _seq(b'nt'))
_NULL_FLOAT_SUFFIX = _NullSequence(IonType.FLOAT, _seq(b'loat'))
_NULL_DECIMAL_SUFFIX = _NullSequence(IonType.DECIMAL, _seq(b'ecimal'))
_NULL_CLOB_SUFFIX = _NullSequence(IonType.CLOB, _seq(b'lob'))
_NULL_LIST_SUFFIX = _NullSequence(IonType.LIST, _seq(b'ist'))
_NULL_BLOB_SUFFIX = _NullSequence(IonType.BLOB, _seq(b'ob'))
_NULL_BOOL_SUFFIX = _NullSequence(IonType.BOOL, _seq(b'ol'))
_NULL_TIMESTAMP_SUFFIX = _NullSequence(IonType.TIMESTAMP, _seq(b'imestamp'))


# The following implements a prefix tree used to determine whether a typed null keyword has been found (see
# _typed_null_handler). The leaves of the tree (enumerated above) are the terminal character sequences for the 13
# possible suffixes to 'null.'. Any other suffix to 'null.' is an error. _NULL_STARTS is entered when 'null.' is found.

_NULL_STR_NEXT = {
    _ord(b'i'): _NULL_STRING_SUFFIX,
    _ord(b'u'): _NULL_STRUCT_SUFFIX
}

_NULL_ST_NEXT = {
    _ord(b'r'): _NULL_STR_NEXT
}

_NULL_S_NEXT = {
    _ord(b'y'): _NULL_SYMBOL_SUFFIX,
    _ord(b'e'): _NULL_SEXP_SUFFIX,
    _ord(b't'): _NULL_ST_NEXT
}

_NULL_B_NEXT = {
    _ord(b'l'): _NULL_BLOB_SUFFIX,
    _ord(b'o'): _NULL_BOOL_SUFFIX
}

_NULL_STARTS = {
    _ord(b'n'): _NULL_SUFFIX,  # null.null
    _ord(b's'): _NULL_S_NEXT,  # null.string, null.symbol, null.struct, null.sexp
    _ord(b'i'): _NULL_INT_SUFFIX,  # null.int
    _ord(b'f'): _NULL_FLOAT_SUFFIX,  # null.float
    _ord(b'd'): _NULL_DECIMAL_SUFFIX,  # null.decimal
    _ord(b'b'): _NULL_B_NEXT,  # null.bool, null.blob
    _ord(b'c'): _NULL_CLOB_SUFFIX,  # null.clob
    _ord(b'l'): _NULL_LIST_SUFFIX,  # null.list
    _ord(b't'): _NULL_TIMESTAMP_SUFFIX,  # null.timestamp
}


class _ContainerContext(record(
    'end', 'delimiter', 'ion_type', 'is_delimited'
)):
    """A description of an Ion container, including the container's IonType and its textual delimiter and end character,
    if applicable.

    This is tracked as part of the current token's context, and is useful when certain lexing decisions depend on
    which container the token is a member of. For example, ending a numeric token with ']' is not legal unless that
    token is contained in a list.

    Args:
        end (tuple): Tuple containing the container's end character, if any.
        delimiter (tuple): Tuple containing the container's delimiter character, if any.
        ion_type (Optional[IonType]): The container's IonType, if any.
        is_delimited (bool): True if delimiter is not empty; otherwise, False.
    """

_C_TOP_LEVEL = _ContainerContext((), (), None, False)
_C_STRUCT = _ContainerContext((_CLOSE_BRACE,), (_COMMA,), IonType.STRUCT, True)
_C_LIST = _ContainerContext((_CLOSE_BRACKET,), (_COMMA,), IonType.LIST, True)
_C_SEXP = _ContainerContext((_CLOSE_PAREN,), (), IonType.SEXP, False)


def _is_escaped(c):
    """Queries whether a character ordinal or code point was part of an escape sequence."""
    try:
        return c.is_escaped
    except AttributeError:
        return False


class _CodePointHolder:
    """Holds a _CodePoint for passing between co-routines."""
    def __init__(self):
        self.code_point = None


def _as_symbol(value):
    if hasattr(value, 'as_symbol'):
        return value.as_symbol()
    assert isinstance(value, SymbolToken)
    return value


class _HandlerContext(record(
    'container', 'queue', 'field_name', 'annotations', 'depth', 'whence',
    'value', 'ion_type', 'pending_symbol', ('quoted_text', False), ('line_comment', False)
)):
    """A context for a handler co-routine.

    Args:
        container (_ContainerContext): The description of the container in which this context is contained.
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
        quoted_text (Optional[bool]): True if this context represents quoted text; otherwise, False.
        line_comment (Optional[bool]): True if this context represents a line comment; otherwise, False.
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

        if ion_type is IonType.SYMBOL and value is _IVM_TOKEN and not annotations and depth == 0:
            return trans_cls(ION_VERSION_MARKER_EVENT, whence), None

        return trans_cls(
            event_cls(event_type, ion_type, value, self.field_name, annotations, depth),
            whence
        ), None

    def immediate_transition(self, delegate, trans_cls=Transition):
        """Returns an immediate transition to another co-routine."""
        return trans_cls(None, delegate), self

    def read_data_event(self, whence, complete=False, can_flush=False):
        """Creates a co-routine for retrieving data as bytes.

        Args:
            whence (Coroutine): The co-routine to return to after the data is satisfied.
            complete (Optional[bool]): True if STREAM_END should be emitted if no bytes are read or
                available; False if INCOMPLETE should be emitted in that case.
            can_flush (Optional[bool]): True if NEXT may be requested after INCOMPLETE is emitted as a result of this
                data request.
        """
        return Transition(None, _read_data_handler(whence, self, complete, can_flush))

    def next_code_point(self, whence):
        """Creates a co-routine for retrieving data as code points.

        This should be used in quoted string contexts.
        """
        out = _CodePointHolder()
        return Transition(None, _next_code_point_handler(whence, self, out)), out

    def derive_unicode(self, quoted_text=False):
        """Derives a context for text values, indicating whether the text is quoted."""
        if isinstance(self.value, CodePointArray):
            assert self.quoted_text == quoted_text
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
            self.pending_symbol,
            quoted_text
        )

    def derive_unquoted_text(self):
        """Derives an unquoted text context. Useful when the current context represents quoted text, and the quoted
        text is exited.
        """
        return _HandlerContext(
            self.container,
            self.queue,
            self.field_name,
            self.annotations,
            self.depth,
            self.whence,
            self.value,
            self.ion_type,
            self.pending_symbol,
        )

    def derive_container_context(self, ion_type, whence, add_depth=1):
        """Derives a container context as a child of the current context."""
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
        """Derives a scalar context as a child of the current context."""
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

    def derive_line_comment(self, is_line_comment=True):
        return _HandlerContext(
            self.container,
            self.queue,
            self.field_name,
            self.annotations,
            self.depth,
            self.whence,
            self.value,
            self.ion_type,
            self.pending_symbol,
            self.quoted_text,
            is_line_comment
        )

    def derive_ion_type(self, ion_type):
        """Derives a context with the given IonType."""
        if ion_type is self.ion_type:
            return self
        return _HandlerContext(
            self.container,
            self.queue,
            self.field_name,
            self.annotations,
            self.depth,
            self.whence,
            self.value,
            ion_type,
            self.pending_symbol,
            self.quoted_text
        )

    def derive_annotation(self):
        """Derives a context which appends the current context's pending_symbol to its annotations sequence."""
        assert self.pending_symbol is not None
        assert not self.value
        annotations = (_as_symbol(self.pending_symbol),)  # pending_symbol becomes an annotation
        return _HandlerContext(
            self.container,
            self.queue,
            self.field_name,
            annotations if not self.annotations else self.annotations + annotations,
            self.depth,
            self.whence,
            self.value,
            None,
            None,  # reset pending symbol
        )

    def derive_field_name(self):
        """Derives a context which sets the current context's pending_symbol as its field name."""
        assert self.pending_symbol is not None
        assert not self.value
        return _HandlerContext(
            self.container,
            self.queue,
            _as_symbol(self.pending_symbol),  # pending_symbol becomes field name
            self.annotations,
            self.depth,
            self.whence,
            self.value,
            self.ion_type,
            None,  # reset pending symbol
        )

    def derive_pending_symbol(self, pending_symbol=None):
        """Derives a context with the given pending_symbol, and resets the context's value."""
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
            pending_symbol,
            self.quoted_text
        )


def _composite_transition(event, ctx, next_handler, next_ctx=None):
    """Composes an event transition followed by an immediate transition to the handler for the next token.

    This is useful when some lookahead is required to determine if a token has ended, e.g. in the case of long strings.
    """
    if next_ctx is None:
        next_ctx = ctx.derive_child_context(ctx.whence)
    return [event, next_ctx.immediate_transition(next_handler(next_ctx))[0]], next_ctx


class _SelfDelimitingTransition(Transition):
    """Signals that this transition terminates token that is self-delimiting, e.g. short string, container, comment."""


def _decode(value):
    return value.decode(_ENCODING)


def _parse_number(parse_func, value, base=10):
    def parse():
        return parse_func(value, base)
    return parse


def _base_10(parse_func, value, base, decode=False):
    assert base == 10
    if decode:
        value = _decode(value)
    return parse_func(value)


def _base_n(parse_func, value, base):
    return parse_func(_decode(value), base)


# In Python 2, int() returns a long if the input overflows an int.
_parse_decimal_int = partial(_parse_number, partial(_base_10, int))
_parse_binary_int = partial(_parse_number, partial(_base_n, int), base=2)
_parse_hex_int = partial(_parse_number, partial(_base_n, int), base=16)
_parse_float = partial(_parse_number, partial(_base_10, float))
_parse_decimal = partial(_parse_number, partial(_base_10, Decimal, decode=True))


@coroutine
def _number_negative_start_handler(c, ctx):
    """Handles numeric values that start with a negative sign. Branches to delegate co-routines according to
    _NEGATIVE_TABLE.
    """
    assert c == _MINUS
    assert len(ctx.value) == 0
    ctx = ctx.derive_ion_type(IonType.INT)
    ctx.value.append(c)
    c, _ = yield
    yield ctx.immediate_transition(_NEGATIVE_TABLE[c](c, ctx))


@coroutine
def _number_zero_start_handler(c, ctx):
    """Handles numeric values that start with zero or negative zero. Branches to delegate co-routines according to
    _ZERO_START_TABLE.
    """
    assert c == _ZERO
    assert len(ctx.value) == 0 or (len(ctx.value) == 1 and ctx.value[0] == _MINUS)
    ctx = ctx.derive_ion_type(IonType.INT)
    ctx.value.append(c)
    c, _ = yield
    if _ends_value(c):
        trans = ctx.event_transition(IonThunkEvent, IonEventType.SCALAR, ctx.ion_type, _parse_decimal_int(ctx.value))
        if c == _SLASH:
            trans = ctx.immediate_transition(_number_slash_end_handler(c, ctx, trans))
        yield trans
    yield ctx.immediate_transition(_ZERO_START_TABLE[c](c, ctx))


@coroutine
def _number_or_timestamp_handler(c, ctx):
    """Handles numeric values that start with digits 1-9. May terminate a value, in which case that value is an
    int. If it does not terminate a value, it branches to delegate co-routines according to _NUMBER_OR_TIMESTAMP_TABLE.
    """
    assert c in _DIGITS
    ctx = ctx.derive_ion_type(IonType.INT)  # If this is the last digit read, this value is an Int.
    val = ctx.value
    val.append(c)
    c, self = yield
    trans = ctx.immediate_transition(self)
    while True:
        if _ends_value(c):
            trans = ctx.event_transition(IonThunkEvent, IonEventType.SCALAR,
                                         ctx.ion_type, _parse_decimal_int(ctx.value))
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
    """Handles numeric values that end in a forward slash. This is only legal if the slash begins a comment; thus,
    this co-routine either results in an error being raised or an event being yielded.
    """
    assert c == _SLASH
    c, self = yield
    next_ctx = ctx.derive_child_context(ctx.whence)
    comment = _comment_handler(_SLASH, next_ctx, next_ctx.whence)
    comment.send((c, comment))
    # If the previous line returns without error, it's a valid comment and the number may be emitted.
    yield [event[0], next_ctx.immediate_transition(comment)[0]], next_ctx


def _numeric_handler_factory(charset, transition, assertion, illegal_before_underscore, parse_func,
                             illegal_at_end=(None,), ion_type=None, append_first_if_not=None, first_char=None):
    """Generates a handler co-routine which tokenizes a numeric component."""
    @coroutine
    def numeric_handler(c, ctx):
        assert assertion(c, ctx)
        if ion_type is not None:
            ctx = ctx.derive_ion_type(ion_type)
        val = ctx.value
        if c != append_first_if_not:
            first = c if first_char is None else first_char
            val.append(first)
        prev = c
        c, self = yield
        trans = ctx.immediate_transition(self)
        while True:
            if _ends_value(c):
                if prev == _UNDERSCORE or prev in illegal_at_end:
                    _illegal_character(c, ctx, '%s at end of number.' % (_chr(prev),))
                trans = ctx.event_transition(IonThunkEvent, IonEventType.SCALAR, ctx.ion_type, parse_func(ctx.value))
                if c == _SLASH:
                    trans = ctx.immediate_transition(_number_slash_end_handler(c, ctx, trans))
            else:
                if c == _UNDERSCORE:
                    if prev == _UNDERSCORE or prev in illegal_before_underscore:
                        _illegal_character(c, ctx, 'Underscore after %s.' % (_chr(prev),))
                else:
                    if c not in charset:
                        trans = transition(prev, c, ctx, trans)
                    else:
                        val.append(c)
            prev = c
            c, _ = yield trans
    return numeric_handler


def _exponent_handler_factory(ion_type, exp_chars, parse_func, first_char=None):
    """Generates a handler co-routine which tokenizes an numeric exponent."""
    def transition(prev, c, ctx, trans):
        if c in _SIGN and prev in exp_chars:
            ctx.value.append(c)
        else:
            _illegal_character(c, ctx)
        return trans
    illegal = exp_chars + _SIGN
    return _numeric_handler_factory(_DIGITS, transition, lambda c, ctx: c in exp_chars, illegal, parse_func,
                                    illegal_at_end=illegal, ion_type=ion_type, first_char=first_char)


def _coefficient_handler_factory(trans_table, parse_func, assertion=lambda c, ctx: True,
                                 ion_type=None, append_first_if_not=None):
    """Generates a handler co-routine which tokenizes a numeric coefficient."""
    def transition(prev, c, ctx, trans):
        if prev == _UNDERSCORE:
            _illegal_character(c, ctx, 'Underscore before %s.' % (_chr(c),))
        return ctx.immediate_transition(trans_table[c](c, ctx))
    return _numeric_handler_factory(_DIGITS, transition, assertion, (_DOT,), parse_func,
                                    ion_type=ion_type, append_first_if_not=append_first_if_not)


def _radix_int_handler_factory(radix_indicators, charset, parse_func):
    """Generates a handler co-routine which tokenizes a integer of a particular radix."""
    def assertion(c, ctx):
        return c in radix_indicators and \
               ((len(ctx.value) == 1 and ctx.value[0] == _ZERO) or
                (len(ctx.value) == 2 and ctx.value[0] == _MINUS and ctx.value[1] == _ZERO)) and \
               ctx.ion_type == IonType.INT
    return _numeric_handler_factory(charset, lambda prev, c, ctx: _illegal_character(c, ctx),
                                    assertion, radix_indicators, parse_func, illegal_at_end=radix_indicators)


_decimal_handler = _exponent_handler_factory(IonType.DECIMAL, _DECIMAL_EXPS, _parse_decimal, first_char=_ord(b'e'))
_float_handler = _exponent_handler_factory(IonType.FLOAT, _FLOAT_EXPS, _parse_float)


_FRACTIONAL_NUMBER_TABLE = _defaultdict(
    _merge_mappings(
        (_DECIMAL_EXPS, _decimal_handler),
        (_FLOAT_EXPS, _float_handler)
    )
)

fractional_number_handler = _coefficient_handler_factory(
    _FRACTIONAL_NUMBER_TABLE, _parse_decimal, assertion=lambda c, ctx: c == _DOT, ion_type=IonType.DECIMAL)

_WHOLE_NUMBER_TABLE = _defaultdict(
    _merge_mappings(
        {
            _DOT: fractional_number_handler,
        },
        _FRACTIONAL_NUMBER_TABLE
    )
)

_whole_number_handler = _coefficient_handler_factory(_WHOLE_NUMBER_TABLE, _parse_decimal_int,
                                                     append_first_if_not=_UNDERSCORE)
_binary_int_handler = _radix_int_handler_factory(_BINARY_RADIX, _BINARY_DIGITS, _parse_binary_int)
_hex_int_handler = _radix_int_handler_factory(_HEX_RADIX, _HEX_DIGITS, _parse_hex_int)


@coroutine
def _timestamp_zero_start_handler(c, ctx):
    """Handles numeric values that start with a zero followed by another digit. This is either a timestamp or an
    error.
    """
    val = ctx.value
    ctx = ctx.derive_ion_type(IonType.TIMESTAMP)
    if val[0] == _MINUS:
        _illegal_character(c, ctx, 'Negative year not allowed.')
    val.append(c)
    c, self = yield
    trans = ctx.immediate_transition(self)
    while True:
        if c in _TIMESTAMP_YEAR_DELIMITERS:
            trans = ctx.immediate_transition(_timestamp_handler(c, ctx))
        elif c in _DIGITS:
            val.append(c)
        else:
            _illegal_character(c, ctx)
        c, _ = yield trans


class _TimestampState(Enum):
    YEAR = 0
    MONTH = 1
    DAY = 2
    HOUR = 3
    MINUTE = 4
    SECOND = 5
    FRACTIONAL = 6
    OFF_HOUR = 7
    OFF_MINUTE = 8


class _TimestampTokens:
    """Holds the individual numeric tokens (as strings) that compose a `Timestamp`."""
    def __init__(self, year=None):
        fld = []
        for i in iter(_TimestampState):
            fld.append(None)
        if year is not None:
            fld[_TimestampState.YEAR] = year
        self._fields = fld

    def transition(self, state):
        val = bytearray()
        self._fields[state] = val
        return val

    def __getitem__(self, item):
        return self._fields[item]


_ZEROS = [
    b'',
    b'0',
    b'00',
    b'000',
    b'0000',
    b'00000'
]

_MICROSECOND_MAGNITUDE = 6


def _parse_timestamp(tokens):
    """Parses each token in the given `_TimestampTokens` and marshals the numeric components into a `Timestamp`."""
    def parse():
        precision = TimestampPrecision.YEAR
        off_hour = tokens[_TimestampState.OFF_HOUR]
        off_minutes = tokens[_TimestampState.OFF_MINUTE]
        if off_hour is not None:
            assert off_minutes is not None
            off_sign = -1 if _MINUS in off_hour else 1
            off_hour = int(off_hour)
            off_minutes = int(off_minutes) * off_sign
            if off_sign == -1 and off_hour == 0 and off_minutes == 0:
                # -00:00 (unknown UTC offset) is a naive datetime.
                off_hour = None
                off_minutes = None
        else:
            assert off_minutes is None

        year = tokens[_TimestampState.YEAR]
        assert year is not None
        year = int(year)

        month = tokens[_TimestampState.MONTH]
        if month is None:
            month = 1
        else:
            month = int(month)
            precision = TimestampPrecision.MONTH

        day = tokens[_TimestampState.DAY]
        if day is None:
            day = 1
        else:
            day = int(day)
            precision = TimestampPrecision.DAY

        hour = tokens[_TimestampState.HOUR]
        minute = tokens[_TimestampState.MINUTE]
        if hour is None:
            assert minute is None
            hour = 0
            minute = 0
        else:
            assert minute is not None
            hour = int(hour)
            minute = int(minute)
            precision = TimestampPrecision.MINUTE

        second = tokens[_TimestampState.SECOND]
        if second is None:
            second = 0
        else:
            second = int(second)
            precision = TimestampPrecision.SECOND

        fraction = tokens[_TimestampState.FRACTIONAL]
        if fraction is None:
            microsecond = 0
        else:
            fraction_digits = len(fraction)
            if fraction_digits > _MICROSECOND_MAGNITUDE:
                for digit in fraction[_MICROSECOND_MAGNITUDE:]:
                    if digit != _ZERO:
                        raise ValueError('Only six significant digits supported in timestamp fractional. Found %s.'
                                         % (fraction,))
                fraction = fraction[0:_MICROSECOND_MAGNITUDE]
            else:
                fraction.extend(_ZEROS[_MICROSECOND_MAGNITUDE - fraction_digits])
            microsecond = int(fraction)
        return timestamp(
            year, month, day,
            hour, minute, second, microsecond,
            off_hour, off_minutes,
            precision=precision
        )
    return parse


@coroutine
def _timestamp_handler(c, ctx):
    """Handles timestamp values. Entered after the year component has been completed; tokenizes the remaining
    components.
    """
    assert c in _TIMESTAMP_YEAR_DELIMITERS
    ctx = ctx.derive_ion_type(IonType.TIMESTAMP)
    if len(ctx.value) != 4:
        _illegal_character(c, ctx, 'Timestamp year is %d digits; expected 4.' % (len(ctx.value),))
    prev = c
    c, self = yield
    trans = ctx.immediate_transition(self)
    state = _TimestampState.YEAR
    nxt = _DIGITS
    tokens = _TimestampTokens(ctx.value)
    val = None
    can_terminate = False
    if prev == _T:
        nxt += _VALUE_TERMINATORS
        can_terminate = True
    while True:
        is_eof = can_terminate and BufferQueue.is_eof(c)
        if c not in nxt and not is_eof:
            _illegal_character(c, ctx, 'Expected %r in state %r.' % ([_chr(x) for x in nxt], state))
        if c in _VALUE_TERMINATORS or is_eof:
            trans = ctx.event_transition(IonThunkEvent, IonEventType.SCALAR, ctx.ion_type, _parse_timestamp(tokens))
            if c == _SLASH:
                trans = ctx.immediate_transition(_number_slash_end_handler(c, ctx, trans))
        else:
            can_terminate = False
            if c == _Z:
                nxt = _VALUE_TERMINATORS
                can_terminate = True
            elif c == _T:
                nxt = _VALUE_TERMINATORS + _DIGITS
                can_terminate = True
            elif c in _TIMESTAMP_DELIMITERS:
                nxt = _DIGITS
            elif c in _DIGITS:
                if prev == _PLUS or (state > _TimestampState.MONTH and prev == _HYPHEN):
                    state = _TimestampState.OFF_HOUR
                    val = tokens.transition(state)
                    if prev == _HYPHEN:
                        val.append(prev)
                elif prev in (_TIMESTAMP_DELIMITERS + (_T,)):
                    state = _TimestampState[state + 1]
                    val = tokens.transition(state)
                    if state == _TimestampState.FRACTIONAL:
                        nxt = _DIGITS + _TIMESTAMP_OFFSET_INDICATORS
                elif prev in _DIGITS:
                    if state == _TimestampState.MONTH:
                        nxt = _TIMESTAMP_YEAR_DELIMITERS
                    elif state == _TimestampState.DAY:
                        nxt = (_T,) + _VALUE_TERMINATORS
                        can_terminate = True
                    elif state == _TimestampState.HOUR:
                        nxt = (_COLON,)
                    elif state == _TimestampState.MINUTE:
                        nxt = _TIMESTAMP_OFFSET_INDICATORS + (_COLON,)
                    elif state == _TimestampState.SECOND:
                        nxt = _TIMESTAMP_OFFSET_INDICATORS + (_DOT,)
                    elif state == _TimestampState.FRACTIONAL:
                        nxt = _DIGITS + _TIMESTAMP_OFFSET_INDICATORS
                    elif state == _TimestampState.OFF_HOUR:
                        nxt = (_COLON,)
                    elif state == _TimestampState.OFF_MINUTE:
                        nxt = _VALUE_TERMINATORS
                        can_terminate = True
                    else:
                        raise ValueError('Unknown timestamp state %r.' % (state,))
                else:
                    # Reaching this branch would be indicative of a programming error within this state machine.
                    raise ValueError('Digit following %s in timestamp state %r.' % (_chr(prev), state))
                val.append(c)
        prev = c
        c, _ = yield trans


@coroutine
def _comment_handler(c, ctx, whence):
    """Handles comments. Upon completion of the comment, immediately transitions back to `whence`."""
    assert c == _SLASH
    c, self = yield
    if c == _SLASH:
        ctx = ctx.derive_line_comment()
        block_comment = False
    elif c == _ASTERISK:
        if ctx.line_comment:
            # This happens when a block comment immediately follows a line comment.
            ctx = ctx.derive_line_comment(False)
        block_comment = True
    else:
        _illegal_character(c, ctx, 'Illegal character sequence "/%s".' % (_chr(c),))
    done = False
    prev = None
    trans = ctx.immediate_transition(self)
    while not done:
        c, _ = yield trans
        if block_comment:
            if prev == _ASTERISK and c == _SLASH:
                done = True
            prev = c
        else:
            if c == _NEWLINE or BufferQueue.is_eof(c):
                done = True
    yield ctx.immediate_transition(whence, trans_cls=_SelfDelimitingTransition)


@coroutine
def _sexp_slash_handler(c, ctx, whence=None, pending_event=None):
    """Handles the special case of a forward-slash within an s-expression. This is either an operator or a
    comment.
    """
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


_SINGLE_QUOTES = [
    b"",
    b"'",
    b"''"
]


def _validate_quoted_text(allowed_whitespace, c, ctx, max_char):
    if c not in allowed_whitespace and not _is_escaped(c) and \
            (c < _MIN_QUOTED_CHAR or c > max_char):
        _illegal_character(c, ctx, 'Character out of range [%d, %d] for this type.'
                           % (_MIN_QUOTED_CHAR, max_char,))

_validate_long_string_text = partial(_validate_quoted_text, _WHITESPACE)


@coroutine
def _long_string_handler(c, ctx, is_field_name=False):
    """Handles triple-quoted strings. Remains active until a value other than a long string is encountered."""
    assert c == _SINGLE_QUOTE
    is_clob = ctx.ion_type is IonType.CLOB
    max_char = _MAX_CLOB_CHAR if is_clob else _MAX_TEXT_CHAR
    assert not (is_clob and is_field_name)
    if not is_clob and not is_field_name:
        ctx = ctx.derive_ion_type(IonType.STRING)
    assert not ctx.value
    ctx_in_data = ctx.derive_unicode(quoted_text=True)
    ctx = ctx_in_data
    val = ctx.value
    if is_field_name:
        assert not val
        ctx = ctx.derive_pending_symbol()
        val = ctx.pending_symbol
    ctx_outside_data = ctx.derive_unquoted_text()
    quotes = 0
    in_data = True
    c, self = yield
    here_in_data = ctx_in_data.immediate_transition(self)
    here_outside_data = ctx_outside_data.immediate_transition(self)
    trans = here_in_data
    while True:
        if c == _SINGLE_QUOTE and not _is_escaped(c):
            quotes += 1
            if quotes == 3:
                in_data = not in_data
                if in_data:
                    trans = here_in_data
                    ctx = ctx_in_data
                else:
                    trans = here_outside_data
                    ctx = ctx_outside_data
                quotes = 0
        else:
            if in_data:
                _validate_long_string_text(c, ctx, max_char)
                # Any quotes found in the meantime are part of the data
                val.extend(_SINGLE_QUOTES[quotes])
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
                                               % (_chr(ctx.container.delimiter[0]),))
                        trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value.as_text())
                        if quotes == 1:
                            if BufferQueue.is_eof(c):
                                _illegal_character(c, ctx, "Unexpected EOF.")
                            # c was read as a single byte. Re-read it as a code point.
                            ctx.queue.unread(c)
                            c, _ = yield here_in_data
                            trans = _composite_transition(
                                trans[0],
                                ctx,
                                partial(_quoted_symbol_handler, c, is_field_name=False),
                            )
                        else:  # quotes == 2
                            trans = trans[0], ctx.derive_child_context(ctx.whence).derive_pending_symbol()
                elif c not in _WHITESPACE:
                    if is_clob:
                        trans = ctx.immediate_transition(_clob_end_handler(c, ctx))
                    elif c == _SLASH:
                        if ctx.container.ion_type is IonType.SEXP:
                            pending = ctx.event_transition(IonEvent, IonEventType.SCALAR,
                                                           ctx.ion_type, ctx.value.as_text())[0]
                            trans = ctx.immediate_transition(_sexp_slash_handler(c, ctx, self, pending))
                        else:
                            trans = ctx.immediate_transition(_comment_handler(c, ctx, self))
                    elif is_field_name:
                        if c != _COLON:
                            _illegal_character(c, ctx, 'Illegal character after field name %s.' % (val,))
                        trans = ctx.immediate_transition(ctx.whence)
                    else:
                        trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value.as_text())
        c, _ = yield trans
        trans = in_data and here_in_data or here_outside_data


@coroutine
def _typed_null_handler(c, ctx):
    """Handles typed null values. Entered once `null.` has been found."""
    assert c == _DOT
    c, self = yield
    nxt = _NULL_STARTS
    i = 0
    length = None
    done = False
    trans = ctx.immediate_transition(self)
    while True:
        if done:
            if _ends_value(c) or (ctx.container.ion_type is IonType.SEXP and c in _OPERATORS):
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
    """Handles the start of an unquoted text token.

    This may be an operator (if in an s-expression), an identifier symbol, or a keyword.
    """
    in_sexp = ctx.container.ion_type is IonType.SEXP
    if c not in _IDENTIFIER_STARTS:
        if in_sexp and c in _OPERATORS:
            c_next, _ = yield
            ctx.queue.unread(c_next)
            yield ctx.immediate_transition(_operator_symbol_handler(c, ctx))
        _illegal_character(c, ctx)
    assert not ctx.value
    ctx = ctx.derive_unicode().derive_ion_type(IonType.SYMBOL)
    val = ctx.value
    val.append(c)
    maybe_null = c == _N_LOWER
    maybe_nan = maybe_null
    maybe_true = c == _T_LOWER
    maybe_false = c == _F_LOWER
    c, self = yield
    trans = ctx.immediate_transition(self)
    keyword_trans = None
    match_index = 0
    while True:
        def check_keyword(name, keyword_sequence, ion_type, value, match_transition=lambda: None):
            maybe_keyword = True
            transition = None
            if match_index < len(keyword_sequence):
                maybe_keyword = c == keyword_sequence[match_index]
            else:
                transition = match_transition()
                if transition is not None:
                    pass
                elif _ends_value(c):
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
                return transition
            maybe_null, keyword_trans = check_keyword('null', _NULL_SUFFIX.sequence,
                                                      IonType.NULL, None, check_null_dot)
        if maybe_nan:
            maybe_nan, keyword_trans = check_keyword('nan', _NAN_SUFFIX, IonType.FLOAT, _NAN)
        elif maybe_true:
            maybe_true, keyword_trans = check_keyword('true', _TRUE_SUFFIX, IonType.BOOL, True)
        elif maybe_false:
            maybe_false, keyword_trans = check_keyword('false', _FALSE_SUFFIX, IonType.BOOL, False)
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
            elif _ends_value(c) or (in_sexp and c in _OPERATORS):
                trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL, val.as_symbol())
            else:
                trans = ctx.immediate_transition(_unquoted_symbol_handler(c, ctx, is_field_name=is_field_name))
        c, _ = yield trans


def _inf_or_operator_handler_factory(c_start, is_delegate=True):
    """Generates handler co-routines for values that may be `+inf` or `-inf`."""
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
        ctx = ctx.derive_ion_type(IonType.FLOAT)
        match_index = 0
        trans = ctx.immediate_transition(self)
        while True:
            if maybe_inf:
                if match_index < len(_INF_SUFFIX):
                    maybe_inf = c == _INF_SUFFIX[match_index]
                else:
                    if _ends_value(c) or (ctx.container.ion_type is IonType.SEXP and c in _OPERATORS):
                        yield ctx.event_transition(
                            IonEvent, IonEventType.SCALAR, IonType.FLOAT, c_start == _MINUS and _NEG_INF or _POS_INF
                        )
                    else:
                        maybe_inf = False
            if maybe_inf:
                match_index += 1
            else:
                ctx = ctx.derive_unicode()
                if match_index > 0:
                    next_ctx = ctx.derive_child_context(ctx.whence)
                    for ch in _INF_SUFFIX[0:match_index]:
                        next_ctx.value.append(ch)
                break
            c, self = yield trans
        if ctx.container is not _C_SEXP:
            _illegal_character(c, next_ctx is None and ctx or next_ctx,
                               'Illegal character following %s.' % (_chr(c_start),))
        if match_index == 0:
            if c in _OPERATORS:
                yield ctx.immediate_transition(_operator_symbol_handler(c, ctx))
            yield ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL, ctx.value.as_symbol())
        yield _composite_transition(
            ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL, ctx.value.as_symbol())[0],
            ctx,
            partial(_unquoted_symbol_handler, c),
            next_ctx
        )
    return inf_or_operator_handler


_negative_inf_or_sexp_hyphen_handler = _inf_or_operator_handler_factory(_MINUS)
_positive_inf_or_sexp_plus_handler = _inf_or_operator_handler_factory(_PLUS, is_delegate=False)


@coroutine
def _operator_symbol_handler(c, ctx):
    """Handles operator symbol values within s-expressions."""
    assert c in _OPERATORS
    ctx = ctx.derive_unicode()
    val = ctx.value
    val.append(c)
    c, self = yield
    trans = ctx.immediate_transition(self)
    while c in _OPERATORS:
        val.append(c)
        c, _ = yield trans
    yield ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL, val.as_symbol())


def _symbol_token_end(c, ctx, is_field_name, trans_cls=Transition, value=None):
    """Returns a transition which ends the current symbol token."""
    if value is None:
        value = ctx.value
    if ctx.quoted_text:
        ctx = ctx.derive_unquoted_text()
    if is_field_name or c in _SYMBOL_TOKEN_TERMINATORS or trans_cls is _SelfDelimitingTransition:
        # This might be an annotation or a field name.
        ctx = ctx.derive_pending_symbol(value)
        trans = ctx.immediate_transition(ctx.whence, trans_cls=trans_cls)
    else:
        trans = ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL,
                                     _as_symbol(value), trans_cls=trans_cls)
    return trans


@coroutine
def _unquoted_symbol_handler(c, ctx, is_field_name=False):
    """Handles identifier symbol tokens. If in an s-expression, these may be followed without whitespace by
    operators.
    """
    in_sexp = ctx.container.ion_type is IonType.SEXP
    ctx = ctx.derive_unicode()
    if c not in _IDENTIFIER_CHARACTERS:
        if in_sexp and c in _OPERATORS:
            c_next, _ = yield
            ctx.queue.unread(c_next)
            assert ctx.value
            yield _composite_transition(
                ctx.event_transition(IonEvent, IonEventType.SCALAR, IonType.SYMBOL, ctx.value.as_symbol())[0],
                ctx,
                partial(_operator_symbol_handler, c)
            )
        _illegal_character(c, ctx.derive_ion_type(IonType.SYMBOL))
    val = ctx.value
    val.append(c)
    prev = c
    c, self = yield
    trans = ctx.immediate_transition(self)
    while True:
        if c not in _WHITESPACE:
            if prev in _WHITESPACE or _ends_value(c) or c == _COLON or (in_sexp and c in _OPERATORS):
                break
            if c not in _IDENTIFIER_CHARACTERS:
                _illegal_character(c, ctx.derive_ion_type(IonType.SYMBOL))
            val.append(c)
        prev = c
        c, _ = yield trans
    yield _symbol_token_end(c, ctx, is_field_name)


@coroutine
def _symbol_identifier_or_unquoted_symbol_handler(c, ctx, is_field_name=False):
    """Handles symbol tokens that begin with a dollar sign. These may end up being system symbols ($ion_*), symbol
    identifiers ('$' DIGITS+), or regular unquoted symbols.
    """
    assert c == _DOLLAR_SIGN
    in_sexp = ctx.container.ion_type is IonType.SEXP
    ctx = ctx.derive_unicode().derive_ion_type(IonType.SYMBOL)
    val = ctx.value
    val.append(c)
    prev = c
    c, self = yield
    trans = ctx.immediate_transition(self)
    maybe_ivm = ctx.depth == 0 and not is_field_name
    maybe_identifier = True
    match_index = 1
    while True:
        if c not in _WHITESPACE:
            if prev in _WHITESPACE or _ends_value(c) or c == _COLON or (in_sexp and c in _OPERATORS):
                break
            maybe_identifier = maybe_identifier and c in _DIGITS
            if maybe_ivm:
                if match_index < len(_IVM_SUFFIX):
                    maybe_ivm = c == _IVM_SUFFIX[match_index]
                else:
                    maybe_ivm = False
            if maybe_ivm:
                match_index += 1
            elif not maybe_identifier:
                yield ctx.immediate_transition(_unquoted_symbol_handler(c, ctx, is_field_name))
            val.append(c)
        elif match_index < len(_IVM_SUFFIX):
            maybe_ivm = False
        prev = c
        c, _ = yield trans
    if len(val) == 1:
        assert val[0] == _chr(_DOLLAR_SIGN)
    elif maybe_identifier:
        assert not maybe_ivm
        sid = int(val[1:])
        val = SymbolToken(None, sid)
    elif maybe_ivm:
        val = _IVM_TOKEN
    yield _symbol_token_end(c, ctx, is_field_name, value=val)


_validate_short_quoted_text = partial(_validate_quoted_text, _WHITESPACE_NOT_NL)


def _quoted_text_handler_factory(delimiter, assertion, before, after, append_first=True,
                                 on_close=lambda ctx: None):
    """Generates handlers for quoted text tokens (either short strings or quoted symbols)."""
    @coroutine
    def quoted_text_handler(c, ctx, is_field_name=False):
        assert assertion(c)
        is_clob = ctx.ion_type is IonType.CLOB
        max_char = _MAX_CLOB_CHAR if is_clob else _MAX_TEXT_CHAR
        ctx = ctx.derive_unicode(quoted_text=True)
        ctx, val, event_on_close = before(c, ctx, is_field_name, is_clob)
        if append_first:
            val.append(c)
        c, self = yield
        trans = ctx.immediate_transition(self)
        done = False
        while not done:
            if c == delimiter and not _is_escaped(c):
                done = True
                if event_on_close:
                    trans = on_close(ctx)
                else:
                    break
            else:
                _validate_short_quoted_text(c, ctx, max_char)
                val.append(c)
            c, _ = yield trans
        yield after(c, ctx, is_field_name)
    return quoted_text_handler


def _short_string_handler_factory():
    """Generates the short string (double quoted) handler."""
    def before(c, ctx, is_field_name, is_clob):
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
        return ctx.event_transition(IonEvent, IonEventType.SCALAR, ctx.ion_type, ctx.value.as_text(),
                                    trans_cls=_SelfDelimitingTransition)

    def after(c, ctx, is_field_name):
        ctx = ctx.derive_unquoted_text()
        return ctx.immediate_transition(
            is_field_name and ctx.whence or _clob_end_handler(c, ctx, is_self_delimited=True),
            trans_cls=_SelfDelimitingTransition
        )

    return _quoted_text_handler_factory(_DOUBLE_QUOTE, lambda c: c == _DOUBLE_QUOTE, before, after, append_first=False,
                                        on_close=on_close)


_short_string_handler = _short_string_handler_factory()


def _quoted_symbol_handler_factory():
    """Generates the quoted symbol (single quoted) handler."""
    def before(c, ctx, is_field_name, is_clob):
        assert not is_clob
        _validate_short_quoted_text(c, ctx, _MAX_TEXT_CHAR)
        return ctx, ctx.value, False

    return _quoted_text_handler_factory(
        _SINGLE_QUOTE,
        lambda c: (c != _SINGLE_QUOTE or _is_escaped(c)),
        before,
        partial(_symbol_token_end, trans_cls=_SelfDelimitingTransition)
    )

_quoted_symbol_handler = _quoted_symbol_handler_factory()


def _single_quote_handler_factory(on_single_quote, on_other):
    """Generates handlers used for classifying tokens that begin with one or more single quotes."""
    @coroutine
    def single_quote_handler(c, ctx, is_field_name=False):
        assert c == _SINGLE_QUOTE
        c, self = yield
        if c == _SINGLE_QUOTE and not _is_escaped(c):
            yield on_single_quote(c, ctx, is_field_name)
        else:
            ctx = ctx.derive_unicode(quoted_text=True)
            yield on_other(c, ctx, is_field_name)
    return single_quote_handler


_two_single_quotes_handler = _single_quote_handler_factory(
    lambda c, ctx, is_field_name: ctx.derive_unicode(quoted_text=True).immediate_transition(
        _long_string_handler(c, ctx, is_field_name)
    ),
    lambda c, ctx, is_field_name:
        ctx.derive_ion_type(IonType.SYMBOL).derive_pending_symbol().immediate_transition(ctx.whence)  # Empty symbol.
)
_long_string_or_symbol_handler = _single_quote_handler_factory(
    lambda c, ctx, is_field_name:
        ctx.derive_ion_type(IonType.SYMBOL).immediate_transition(_two_single_quotes_handler(c, ctx, is_field_name)),
    lambda c, ctx, is_field_name: ctx.immediate_transition(_quoted_symbol_handler(c, ctx, is_field_name))
)


@coroutine
def _struct_or_lob_handler(c, ctx):
    """Handles tokens that begin with an open brace."""
    assert c == _OPEN_BRACE
    c, self = yield
    yield ctx.immediate_transition(_STRUCT_OR_LOB_TABLE[c](c, ctx))


def _parse_lob(ion_type, value):
    def parse():
        if ion_type is IonType.CLOB:
            byte_value = bytearray()
            for b in value.as_text():
                byte_value.append(ord(b))
            return six.binary_type(byte_value)
        return base64.b64decode(value)
    return parse


@coroutine
def _lob_start_handler(c, ctx):
    """Handles tokens that begin with two open braces."""
    assert c == _OPEN_BRACE
    c, self = yield
    trans = ctx.immediate_transition(self)
    quotes = 0
    while True:
        if c in _WHITESPACE:
            if quotes > 0:
                _illegal_character(c, ctx)
        elif c == _DOUBLE_QUOTE:
            if quotes > 0:
                _illegal_character(c, ctx)
            ctx = ctx.derive_ion_type(IonType.CLOB).derive_unicode(quoted_text=True)
            yield ctx.immediate_transition(_short_string_handler(c, ctx))
        elif c == _SINGLE_QUOTE:
            if not quotes:
                ctx = ctx.derive_ion_type(IonType.CLOB).derive_unicode(quoted_text=True)
            quotes += 1
            if quotes == 3:
                yield ctx.immediate_transition(_long_string_handler(c, ctx))
        else:
            yield ctx.immediate_transition(_blob_end_handler(c, ctx))
        c, _ = yield trans


def _lob_end_handler_factory(ion_type, action, validate=lambda c, ctx, action_res: None):
    """Generates handlers for the end of blob or clob values."""
    assert ion_type is IonType.BLOB or ion_type is IonType.CLOB

    @coroutine
    def lob_end_handler(c, ctx, is_self_delimited=False):
        val = ctx.value
        prev = c
        action_res = None
        if c != _CLOSE_BRACE and c not in _WHITESPACE:
            action_res = action(c, ctx, prev, is_self_delimited, action_res, True)
        c, self = yield
        trans = ctx.immediate_transition(self)
        while True:
            if c in _WHITESPACE:
                if prev == _CLOSE_BRACE:
                    _illegal_character(c, ctx.derive_ion_type(ion_type), 'Expected }.')
            elif c == _CLOSE_BRACE:
                if prev == _CLOSE_BRACE:
                    validate(c, ctx, action_res)
                    break
            else:
                action_res = action(c, ctx, prev, is_self_delimited, action_res, False)
            prev = c
            c, _ = yield trans
        yield ctx.event_transition(IonThunkEvent, IonEventType.SCALAR, ion_type,
                                   _parse_lob(ion_type, val), trans_cls=_SelfDelimitingTransition)
    return lob_end_handler


def _blob_end_handler_factory():
    """Generates the handler for the end of a blob value. This includes the base-64 data and the two closing braces."""
    def expand_res(res):
        if res is None:
            return 0, 0
        return res

    def action(c, ctx, prev, is_self_delimited, res, is_first):
        assert not is_self_delimited
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

    return _lob_end_handler_factory(IonType.BLOB, action, validate)

_blob_end_handler = _blob_end_handler_factory()


def _clob_end_handler_factory():
    """Generates the handler for the end of a clob value. This includes anything from the data's closing quote through
    the second closing brace.
    """
    def action(c, ctx, prev, is_self_delimited, res, is_first):
        if is_first and is_self_delimited and c == _DOUBLE_QUOTE:
            assert c is prev
            return res
        _illegal_character(c, ctx)

    return _lob_end_handler_factory(IonType.CLOB, action)

_clob_end_handler = _clob_end_handler_factory()


_single_quoted_field_name_handler = partial(_long_string_or_symbol_handler, is_field_name=True)
_double_quoted_field_name_handler = partial(_short_string_handler, is_field_name=True)
_unquoted_field_name_handler = partial(_symbol_or_keyword_handler, is_field_name=True)
_symbol_identifier_or_unquoted_field_name_handler = partial(_symbol_identifier_or_unquoted_symbol_handler,
                                                            is_field_name=True)


def _container_start_handler_factory(ion_type, before_yield=lambda c, ctx: None):
    """Generates handlers for tokens that begin with container start characters."""
    assert ion_type.is_container

    @coroutine
    def container_start_handler(c, ctx):
        before_yield(c, ctx)
        yield
        yield ctx.event_transition(IonEvent, IonEventType.CONTAINER_START, ion_type)
    return container_start_handler


# Struct requires unread_byte because we had to read one char past the { to make sure it wasn't a lob.
_struct_handler = _container_start_handler_factory(IonType.STRUCT, lambda c, ctx: ctx.queue.unread(c))
_list_handler = _container_start_handler_factory(IonType.LIST)
_sexp_handler = _container_start_handler_factory(IonType.SEXP)


@coroutine
def _read_data_handler(whence, ctx, complete, can_flush):
    """Creates a co-routine for retrieving data up to a requested size.

    Args:
        whence (Coroutine): The co-routine to return to after the data is satisfied.
        ctx (_HandlerContext): The context for the read.
        complete (True|False): True if STREAM_END should be emitted if no bytes are read or
            available; False if INCOMPLETE should be emitted in that case.
        can_flush (True|False): True if NEXT may be requested after INCOMPLETE is emitted as a result of this data
            request.
    """
    trans = None
    queue = ctx.queue

    while True:
        data_event, self = (yield trans)
        if data_event is not None:
            if data_event.data is not None:
                data = data_event.data
                data_len = len(data)
                if data_len > 0:
                    queue.extend(data)
                    yield Transition(None, whence)
            elif data_event.type is ReadEventType.NEXT:
                queue.mark_eof()
                if not can_flush:
                    _illegal_character(queue.read_byte(), ctx, "Unexpected EOF.")
                yield Transition(None, whence)
        trans = Transition(complete and ION_STREAM_END_EVENT or ION_STREAM_INCOMPLETE_EVENT, self)


_ZERO_START_TABLE = _defaultdict(
    _merge_mappings(
        _WHOLE_NUMBER_TABLE,
        (_DIGITS, _timestamp_zero_start_handler),
        (_BINARY_RADIX, _binary_int_handler),
        (_HEX_RADIX, _hex_int_handler)
    )
)

_NUMBER_OR_TIMESTAMP_TABLE = _defaultdict(
    _merge_mappings(
        {
            _UNDERSCORE: _whole_number_handler,
        },
        _WHOLE_NUMBER_TABLE,
        (_TIMESTAMP_YEAR_DELIMITERS, _timestamp_handler)
    )
)

_NEGATIVE_TABLE = _defaultdict(
    _merge_mappings(
        {
            _ZERO: _number_zero_start_handler,
        },
        (_DIGITS[1:], _whole_number_handler)
    ),
    fallback=_negative_inf_or_sexp_hyphen_handler
)

_STRUCT_OR_LOB_TABLE = _defaultdict({
    _OPEN_BRACE: _lob_start_handler
}, _struct_handler)


_FIELD_NAME_START_TABLE = _defaultdict(
    _merge_mappings(
        {
            _SINGLE_QUOTE: _single_quoted_field_name_handler,
            _DOUBLE_QUOTE: _double_quoted_field_name_handler,
            _DOLLAR_SIGN: _symbol_identifier_or_unquoted_field_name_handler,
        },
        (_IDENTIFIER_STARTS, _unquoted_field_name_handler)
    ),
    fallback=partial(_illegal_character, message='Illegal character in field name.')
)

_VALUE_START_TABLE = _defaultdict(
    _merge_mappings(
        {
            _MINUS: _number_negative_start_handler,
            _PLUS: _positive_inf_or_sexp_plus_handler,
            _ZERO: _number_zero_start_handler,
            _OPEN_BRACE: _struct_or_lob_handler,
            _OPEN_PAREN: _sexp_handler,
            _OPEN_BRACKET: _list_handler,
            _SINGLE_QUOTE: _long_string_or_symbol_handler,
            _DOUBLE_QUOTE: _short_string_handler,
            _DOLLAR_SIGN: _symbol_identifier_or_unquoted_symbol_handler,
        },
        (_DIGITS[1:], _number_or_timestamp_handler)
    ),
    fallback=_symbol_or_keyword_handler
)

_IMMEDIATE_FLUSH_TABLE = _defaultdict(
    _merge_mappings(
        (_DIGITS, True),
        (_LETTERS, True),
        {_DOLLAR_SIGN: True},
    ),
    fallback=lambda: False
)


@coroutine
def _container_handler(c, ctx):
    """Coroutine for container values. Delegates to other coroutines to tokenize all child values."""
    _, self = (yield None)
    queue = ctx.queue
    child_context = None
    is_field_name = ctx.ion_type is IonType.STRUCT
    delimiter_required = False
    complete = ctx.depth == 0
    can_flush = False

    def has_pending_symbol():
        return child_context and child_context.pending_symbol is not None

    def symbol_value_event():
        return child_context.event_transition(
            IonEvent, IonEventType.SCALAR, IonType.SYMBOL, _as_symbol(child_context.pending_symbol))[0]

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
        if c in ctx.container.end or c in ctx.container.delimiter or BufferQueue.is_eof(c):
            symbol_event = pending_symbol_value()
            if symbol_event is not None:
                yield symbol_event
                child_context = None
                delimiter_required = ctx.container.is_delimited
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
            elif c in ctx.container.delimiter:
                if not delimiter_required:
                    _illegal_character(c, ctx.derive_child_context(None),
                                       'Encountered delimiter %s without preceding value.'
                                       % (_chr(ctx.container.delimiter[0]),))
                is_field_name = ctx.ion_type is IonType.STRUCT
                delimiter_required = False
                c = None
            else:
                assert BufferQueue.is_eof(c)
                assert len(queue) == 0
                yield ctx.read_data_event(self, complete=True)
                c = None
        if c is not None and c not in _WHITESPACE:
            can_flush = False
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
                                   % (_chr(ctx.container.delimiter[0]),))
            elif has_pending_symbol():
                # A character besides whitespace, comments, and delimiters has been found, and there is a pending
                # symbol. That pending symbol is either an annotation, a field name, or a symbol value.
                if c == _COLON:
                    if is_field_name:
                        is_field_name = False
                        child_context = child_context.derive_field_name()
                        c = None
                    else:
                        assert not ctx.quoted_text
                        if len(queue) == 0:
                            yield ctx.read_data_event(self)
                        c = queue.read_byte()
                        if c == _COLON:
                            child_context = child_context.derive_annotation()
                            c = None  # forces another character to be read safely
                        else:
                            # Colon that doesn't indicate a field name or annotation.
                            _illegal_character(c, child_context)
                else:
                    if is_field_name:
                        _illegal_character(c, child_context, 'Illegal character after field name %s.'
                                           % child_context.pending_symbol)
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
                    can_flush = _IMMEDIATE_FLUSH_TABLE[c]
            container_start = c == _OPEN_BRACKET or c == _OPEN_PAREN  # Note: '{' not here because that might be a lob
            quoted_start = c == _DOUBLE_QUOTE or c == _SINGLE_QUOTE
            while True:
                # Loop over all characters in the current token. A token is either a non-symbol value or a pending
                # symbol, which may end up being a field name, annotation, or symbol value.
                if container_start:
                    c = None
                    container_start = False
                else:
                    if child_context.quoted_text or quoted_start:
                        quoted_start = False
                        cp_trans, c = child_context.next_code_point(self)
                        if cp_trans is not None:
                            yield cp_trans
                            c = c.code_point
                    else:
                        if len(queue) == 0:
                            yield ctx.read_data_event(self, can_flush=can_flush)
                        c = queue.read_byte()
                trans, child_context = handler.send((c, handler))
                can_flush = child_context is not None and \
                    child_context.depth == 0 and \
                    (
                        (
                            child_context.ion_type is not None and
                            (
                                child_context.ion_type.is_numeric or
                                (child_context.ion_type.is_text and not ctx.quoted_text and not is_field_name)
                            )
                        ) or
                        (
                            child_context.line_comment and
                            not is_value_decorated()
                        )
                    )
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
                    event_ion_type = trans.event.ion_type  # None in the case of IVM event.
                    is_container = event_ion_type is not None and event_ion_type.is_container and \
                        trans.event.event_type is not IonEventType.SCALAR
                    if is_container:
                        assert next_transition is None
                        yield Transition(
                            None,
                            _container_handler(c, ctx.derive_container_context(trans.event.ion_type, self))
                        )
                    complete = ctx.depth == 0
                    if is_container or isinstance(trans, _SelfDelimitingTransition):
                        # The end of the value has been reached, and c needs to be updated
                        assert not ctx.quoted_text
                        if len(queue) == 0:
                            yield ctx.read_data_event(self, complete, can_flush)
                        c = queue.read_byte()
                    delimiter_required = ctx.container.is_delimited
                    if next_transition is None:
                        break
                    else:
                        trans = next_transition
                elif self is trans.delegate:
                    assert next_transition is None
                    child_context = child_context.derive_ion_type(None)  # The next token will determine the type.
                    complete = False
                    self_delimiting = isinstance(trans, _SelfDelimitingTransition)
                    if is_field_name:
                        assert not can_flush
                        if c == _COLON or not self_delimiting:
                            break
                    elif has_pending_symbol():
                        can_flush = ctx.depth == 0
                        if not self_delimiting or child_context.line_comment:
                            break
                    elif self_delimiting:
                        # This is the end of a comment. If this is at the top level and is un-annotated,
                        # it may end the stream.
                        complete = ctx.depth == 0 and not is_value_decorated()
                    # This happens at the end of a comment within this container, or when a symbol token has been
                    # found. In both cases, an event should not be emitted. Read the next character and continue.
                    if len(queue) == 0:
                        yield ctx.read_data_event(self, complete, can_flush)
                    c = queue.read_byte()
                    break
                # This is an immediate transition to a handler (may be the same one) for the current token.
                handler = trans.delegate
        else:
            assert not ctx.quoted_text
            if len(queue) == 0:
                yield ctx.read_data_event(self, complete, can_flush)
            c = queue.read_byte()


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


_next_code_point_iter = partial(_next_code_point, yield_char=UCS2)


@coroutine
def _next_code_point_handler(whence, ctx, out):
    """Retrieves the next code point from within a quoted string or symbol."""
    data_event, self = yield
    queue = ctx.queue
    unicode_escapes_allowed = ctx.ion_type is not IonType.CLOB
    while True:
        if len(queue) == 0:
            yield ctx.read_data_event(self)
        queue_iter = iter(queue)
        code_point_generator = _next_code_point_iter(queue, queue_iter)
        code_point = next(code_point_generator)
        if code_point == _BACKSLASH:
            escape_sequence = b'' + six.int2byte(_BACKSLASH)
            num_digits = None
            escaped_newline = False
            while True:
                if len(queue) == 0:
                    yield ctx.read_data_event(self)
                code_point = next(queue_iter)
                if six.indexbytes(escape_sequence, -1) == _BACKSLASH:
                    if code_point == _ord(b'x'):
                        num_digits = 4  # 2-digit hex escapes
                    elif code_point == _ord(b'u') and unicode_escapes_allowed:
                        num_digits = 6  # 4-digit unicode escapes
                    elif code_point == _ord(b'U') and unicode_escapes_allowed:
                        num_digits = 10  # 8-digit unicode escapes
                    elif code_point in _COMMON_ESCAPES:
                        if code_point == _SLASH or code_point == _QUESTION_MARK:
                            escape_sequence = b''  # Drop the \. Python does not recognize these as escapes.
                        escape_sequence += six.int2byte(code_point)
                        break
                    elif code_point in _NEWLINES:
                        escaped_newline = True
                        if code_point == _CARRIAGE_RETURN:
                            if len(queue) == 0:
                                yield ctx.read_data_event(self)
                            code_point = next(queue_iter)
                            if code_point != _NEWLINE:
                                queue.unread(code_point)
                        break
                    else:
                        # This is a backslash followed by an invalid escape character. This is illegal.
                        _illegal_character(code_point, ctx, 'Invalid escape sequence \\%s.' % (_chr(code_point),))
                    escape_sequence += six.int2byte(code_point)
                else:
                    if code_point not in _HEX_DIGITS:
                        _illegal_character(code_point, ctx,
                                           'Non-hex character %s found in unicode escape.' % (_chr(code_point),))
                    escape_sequence += six.int2byte(code_point)
                    if len(escape_sequence) == num_digits:
                        break
            if escaped_newline:
                continue
            escape_sequence = escape_sequence.decode('unicode-escape')
            cp_iter = unicode_iter(escape_sequence)
            out.code_point = CodePoint(next(cp_iter))
            out.code_point.char = escape_sequence
            out.code_point.is_escaped = True
            yield Transition(None, whence)
        while code_point is None:
            yield ctx.read_data_event(self)
            code_point = next(code_point_generator)
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
            to iterate over values; ``DATA`` or ``NEXT`` if the last event type was ``INCOMPLETE``;
            or ``DATA`` if the last event type was ``STREAM_END``.

            When the reader receives ``NEXT`` after yielding ``INCOMPLETE``, this signals to the reader
            that no further data is coming, and that any pending data should be flushed as either parse
            events or errors. This is **only** valid at the top-level, and will **only** result in a parse
            event if the last character encountered...
                * was a digit or a decimal point in a non-timestamp, non-keyword numeric value; OR
                * ended a valid partial timestamp; OR
                * ended a keyword value (special floats, booleans, ``null``, and typed nulls); OR
                * was part of an unquoted symbol token, or whitespace or the end of a comment following
                  an unquoted symbol token (as long as no colons were encountered after the token); OR
                * was the closing quote of a quoted symbol token, or whitespace or the end of a comment
                  following a quoted symbol token (as long as no colons were encountered after the
                  token); OR
                * was the final closing quote of a long string, or whitespace or the end of a comment
                  following a long string.
            If the reader successfully yields a parse event as a result of this, ``NEXT`` is the only
            input that may immediately follow. At that point, there are only two possible responses from
            the reader:
                * If the last character read was the closing quote of an empty symbol following a long
                  string, the reader will emit a parse event representing a symbol value with empty text.
                  The next reader input/output event pair must be (``NEXT``, ``STREAM_END``).
                * Otherwise, the reader will emit ``STREAM_END``.
            After that ``STREAM_END``, the user may later provide ``DATA`` to resume reading.
            If this occurs, the new data will be interpreted as if it were at the start of the stream
            (i.e. it can never continue the previous value), except that it occurs within the same symbol
            table context. This has the following implications (where ``<FLUSH>`` stands for the
            (``INCOMPLETE``, ``NEXT``) transaction):
                * If the previously-emitted value was a numeric value (``int``, ``float``, ``decimal``,
                  ``timestamp``), the new data will never extend that value, even if it would be a valid
                  continuation. For example, ``123<FLUSH>456`` will always be emitted as two parse events
                  (ints ``123`` and ``456``), even though it would have been interpreted as ``123456``
                  without the ``<FLUSH>``.
                * If the previously-emitted value was a symbol value or long string, the new data will
                  be interpreted as the start of a new value. For example, ``abc<FLUSH>::123`` will be
                  emitted as the symbol value ``'abc'``, followed by an error upon encountering ':' at the
                  start of a value, even though it would have been interpreted as the ``int`` ``123``
                  annotated with ``'abc'`` without the ``<FLUSH>``. The input ``abc<FLUSH>abc`` will be
                  emitted as the symbol value ``'abc'`` (represented by a :class:`SymbolToken`), followed by
                  another symbol value ``'abc'`` (represented by a ``SymbolToken`` with the same symbol ID),
                  even though it would have been interpreted as ``'abcabc'`` without the ``<FLUSH>``.
                  Similarly, ``'''abc'''<FLUSH>'''def'''`` will the interpreted as two strings (``'abc'``
                  and ``'def'``), even though it would have been interpreted as ``'abcdef'`` without the
                  ``<FLUSH>``.

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
    return reader_trampoline(_skip_trampoline(_container_handler(None, ctx)), allow_flush=True)

text_reader = reader
