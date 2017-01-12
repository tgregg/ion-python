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

from functools import partial
from io import BytesIO
from itertools import chain
from os import listdir
from os.path import isfile, join, abspath

from pytest import raises

from amazon.ion.exceptions import IonException
from amazon.ion.equivalence import ion_equals
from amazon.ion.simpleion import load
from amazon.ion.util import Enum
from tests import parametrize


# This file lives in the tests/ directory. Up one level is tests/ and up another level is the package root, which
# contains the vectors/ directory.
_VECTORS_ROOT = abspath(join(abspath(__file__), u'..', u'..', u'vectors', u'iontestdata'))
_GOOD_SUBDIR = (u'good',)
_BAD_SUBDIR = (u'bad',)
_TIMESTAMP_SUBDIR = (u'timestamp',)
_TIMELINE_SUBDIR = (u'equivTimeline',)
_UTF8_SUBDIR = (u'utf8',)
_EQUIVS_SUBDIR = (u'equivs',)
_NONEQUIVS_SUBDIR = (u'non-equivs',)

_embedded_documents = u'embedded_documents'


def _abspath(*subdirectories):
    return join(_VECTORS_ROOT, *subdirectories)


def _abspath_file(subdirectories, f):
    return _abspath(*(subdirectories + (f,)))

_abspath_good = partial(_abspath_file, _GOOD_SUBDIR)
_abspath_bad = partial(_abspath_file, _BAD_SUBDIR)
_abspath_equivs = partial(_abspath_file, _GOOD_SUBDIR + _EQUIVS_SUBDIR)
_abspath_equivs_utf8 = partial(_abspath_file, _GOOD_SUBDIR + _EQUIVS_SUBDIR + _UTF8_SUBDIR)
_abspath_nonequivs = partial(_abspath_file, _GOOD_SUBDIR + _NONEQUIVS_SUBDIR)
_abspath_equiv_timeline = partial(_abspath_file, _GOOD_SUBDIR + _TIMESTAMP_SUBDIR + _TIMELINE_SUBDIR)

_SKIP_LIST = (
    # TEXT:
    _abspath_good(u'subfieldVarUInt.ion'),  # TODO investigate. See also: https://github.com/amznlabs/ion-java/issues/62
    _abspath_good(u'subfieldVarUInt32bit.ion'),  # TODO investigate. See also: https://github.com/amznlabs/ion-java/issues/62
    _abspath_good(u'utf16.ion'),  # TODO see https://github.com/amznlabs/ion-java/issues/61
    _abspath_good(u'utf32.ion'),  # TODO see https://github.com/amznlabs/ion-java/issues/61
    # TODO the following contain invalid max ID values. The spec says to interpret these as undefined max IDs.
    # This implementation raises errors, while java apparently doesn't.
    _abspath_good(u'localSymbolTableImportNegativeMaxId.ion'),
    _abspath_good(u'localSymbolTableImportNonIntegerMaxId.ion'),
    _abspath_good(u'localSymbolTableImportNullMaxId.ion'),
    # TODO the following contain symbol identifiers without a corresponding mapping. The spec says these should be
    # errors (as they are in this implementation).
    # _abspath_good(u'notVersionMarkers.ion'),
    # _abspath_good(u'symbols.ion'),
    # _abspath_nonequivs(u'annotations.ion'),
    # TODO The following contains timestamps that aren't equal to each other according to the spec
    # (different precisions). This should probably be split up and moved. Java handles this by defining a different
    # equivalence for this file which compares the timestamps after converting them to millis. Why is this needed?
    _abspath_equiv_timeline(u'timestamps.ion'),
    # TODO the following contain structs with repeated field names. Simpleion maps these to dicts, whose keys are de-duped.
    _abspath_equivs(u'structsFieldsRepeatedNames.ion'),
    _abspath_nonequivs(u'structs.ion'),

    _abspath_equivs_utf8(u'stringUtf8.ion'),  # TODO some error in parsing the file around unpaired unicode surrogates. Investigate.
    _abspath_equivs_utf8(u'stringU0001D11E.ion'),
    _abspath_equivs_utf8(u'stringU0120.ion'),
    _abspath_equivs_utf8(u'stringU2021.ion'),

    # BINARY:
    _abspath_good(u'nullInt3.10n'),  # TODO the binary reader needs to support the 0x3F type code (null int (negative))
    _abspath_good(u'structAnnotatedOrdered.10n'),  # TODO investigate.
    _abspath_good(u'structOrdered.10n'),  # TODO investigate.
    _abspath_bad(u'minLongWithLenTooSmall.10n'),  # TODO this is no longer "bad" because NOP padding is now allowed.
    _abspath_bad(u'nullBadTD.10n'),  # TODO this is no longer "bad" because NOP padding is now allowed. This should be renamed "NOPPad.10n" and moved to "good".
    # TODO the following contain inaccurate annot_length subfields, which pass in weird ways. Needs to be fixed.
    _abspath_bad(u'container_length_mismatch.10n'),
    _abspath_bad(u'emptyAnnotatedInt.10n'),
    # TODO the last element of the following contains a timestamp with a negative fractional. This is illegal per spec.
    # It should be removed from here and put in its own test under bad/ .
    _abspath_equivs(u'timestampFractions.10n'),
)

_DEBUG_WHITELIST = (
    # Place files here to run only their tests.
)


class _VectorType(Enum):
    GOOD = 0
    GOOD_EQUIVS = 2
    GOOD_NONEQUIVS = 3
    BAD = 4
    BAD_EQUIVS = 5

    @property
    def is_bad(self):
        return self >= _VectorType.BAD

    @property
    def is_equivs(self):
        return self is _VectorType.GOOD_EQUIVS or self is _VectorType.BAD_EQUIVS


class _Parameter:
    def __init__(self, vector_type, file_path, test_thunk, desc=''):
        self.vector_type = vector_type
        self.file_path = file_path
        self.test_thunk = test_thunk
        self.desc = '%s - %s %s' % (vector_type.name, file_path, desc)

    def __str__(self):
        return self.desc

_P = _Parameter
_T = _VectorType


def _list_files(*subdirectories):
    directory_path = _abspath(*subdirectories)
    for file in listdir(directory_path):
        file_path = join(directory_path, file)
        if _DEBUG_WHITELIST:
            if file_path in _DEBUG_WHITELIST:
                yield file_path
        elif isfile(file_path) and file_path not in _SKIP_LIST:
            yield file_path


def _load_thunk(file, is_bad):

    def good():
        vector = open(file, 'rb')
        load(vector, single_value=False)

    def bad():
        vector = open(file, 'rb')
        with raises((IonException, ValueError, TypeError)):
            load(vector, single_value=False)

    return bad if is_bad else good


def _basic(vector_type, *subdirectories):
    for file in _list_files(*subdirectories):
        yield _P(vector_type, file, _load_thunk(file, vector_type.is_bad))


def _equivs_thunk(a, b):
    def assert_equal():
        assert ion_equals(a, b)
    return assert_equal


def _nonequivs_thunk(a, b):
    def assert_nonequal():
        assert not ion_equals(a, b)
    return assert_nonequal


def _equivs_params(file, ion_sequence):
    if ion_sequence.ion_annotations and ion_sequence.ion_annotations[0].text == _embedded_documents:
        pass  # TODO
    else:
        previous = ion_sequence[0]
        for value in ion_sequence:
            yield _P(_T.GOOD_EQUIVS, file, _equivs_thunk(previous, value),
                     desc='%r == %r' % (previous, value))
            previous = value


def _nonequivs_params(file, ion_sequence):
    if ion_sequence.ion_annotations and ion_sequence.ion_annotations[0].text == _embedded_documents:
        pass  # TODO
    else:
        for i in range(len(ion_sequence)):
            for j in range(len(ion_sequence)):
                if i == j:
                    continue
                yield _P(_T.GOOD_NONEQUIVS, file, _nonequivs_thunk(ion_sequence[i], ion_sequence[j]),
                         desc='%r != %r' % (ion_sequence[i], ion_sequence[j]))


def _good_comparisons(vector_type, *subdirectories):
    params = _equivs_params if vector_type.is_equivs else _nonequivs_params
    for file in _list_files(*(_GOOD_SUBDIR + subdirectories)):
        vector = open(file, 'rb')
        sequences = load(vector, single_value=False)
        for sequence in sequences:
            for param in params(file, sequence):
                yield param


_good = partial(_basic, _T.GOOD, *_GOOD_SUBDIR)
_good_timestamp = partial(_basic, _T.GOOD, *(_GOOD_SUBDIR + _TIMESTAMP_SUBDIR))
_good_timestamp_equiv_timeline = partial(_good_comparisons, _T.GOOD_EQUIVS, *(_TIMESTAMP_SUBDIR + _TIMELINE_SUBDIR))
_good_equivs = partial(_good_comparisons, _T.GOOD_EQUIVS, *_EQUIVS_SUBDIR)
_good_equivs_utf8 = partial(_good_comparisons, _T.GOOD_EQUIVS, *(_EQUIVS_SUBDIR + _UTF8_SUBDIR))
_good_nonequivs = partial(_good_comparisons, _T.GOOD_NONEQUIVS, *_NONEQUIVS_SUBDIR)
_bad = partial(_basic, _T.BAD, *_BAD_SUBDIR)
_bad_timestamp = partial(_basic, _T.BAD, *(_BAD_SUBDIR + _TIMESTAMP_SUBDIR))
_bad_utf8 = partial(_basic, _T.BAD, *(_BAD_SUBDIR + _UTF8_SUBDIR))


@parametrize(*chain(
    _good(),
    _good_timestamp(),
    _good_timestamp_equiv_timeline(),
    _good_equivs(),
    _good_equivs_utf8(),
    _good_nonequivs(),
    _bad(),
    _bad_timestamp(),
    _bad_utf8(),
))
def test_all(p):
    p.test_thunk()

