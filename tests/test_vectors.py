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
from amazon.ion.simpleion import load
from amazon.ion.util import Enum
from tests import parametrize


# This file lives in the tests/ directory. Up one level is tests/ and up another level is the package root, which
# contains the vectors/ directory.
_VECTORS_ROOT = abspath(join(abspath(__file__), u'..', u'..', u'vectors', u'iontestdata'))
_GOOD_SUBDIR = (u'good',)
_BAD_SUBDIR = (u'bad',)
_TIMESTAMP_SUBDIR = (u'timestamp',)
_UTF8_SUBDIR = (u'utf8',)


def _abspath(*subdirectories):
    return join(_VECTORS_ROOT, *subdirectories)


def _abspath_file(subdirectories, f):
    return _abspath(*(subdirectories + (f,)))

_abspath_good = partial(_abspath_file, _GOOD_SUBDIR)
_abspath_bad = partial(_abspath_file, _BAD_SUBDIR)

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
    _abspath_good(u'notVersionMarkers.ion'),
    _abspath_good(u'symbols.ion'),
    # TODO these are all "future" IVMs. This implementation treats them as regular symbols while the others error.
    # I think the spec is unclear.
    _abspath_bad(u'invalidVersionMarker_ion_0_0.ion'),
    _abspath_bad(u'invalidVersionMarker_ion_1_1.ion'),
    _abspath_bad(u'invalidVersionMarker_ion_2_0.ion'),
    _abspath_bad(u'invalidVersionMarker_ion_1234_0.ion'),

    # BINARY:
    _abspath_good(u'nullInt3.10n'),  # TODO the binary reader needs to support the 0x3F type code (null int (negative))
    _abspath_good(u'structAnnotatedOrdered.10n'),  # TODO investigate.
    _abspath_good(u'structOrdered.10n'),  # TODO investigate.
    _abspath_bad(u'minLongWithLenTooSmall.10n'),  # TODO this is no longer "bad" because NOP padding is now allowed.
    _abspath_bad(u'nullBadTD.10n'),  # TODO this is no longer "bad" because NOP padding is now allowed. This should be renamed "NOPPad.10n" and moved to "good".
    # TODO the following contain inaccurate annot_length subfields, which pass in weird ways. Needs to be fixed.
    _abspath_bad(u'container_length_mismatch.10n'),
    _abspath_bad(u'emptyAnnotatedInt.10n'),
)


class _VectorType(Enum):
    GOOD = 0
    GOOD_EQUIVS = 2
    BAD = 3
    BAD_EQUIVS = 4

    @property
    def is_bad(self):
        return self >= _VectorType.BAD


class _Parameter:
    def __init__(self, vector_type, file_path, test_thunk):
        self.vector_type = vector_type
        self.file_path = file_path
        self.test_thunk = test_thunk
        self.desc = u'%s - %s' % (vector_type.name, file_path)

    def __str__(self):
        return self.desc

_P = _Parameter
_T = _VectorType


def _list_files(*subdirectories):
    directory_path = _abspath(*subdirectories)
    for file in listdir(directory_path):
        file_path = join(directory_path, file)
        if isfile(file_path) and file_path not in _SKIP_LIST:
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

_good = partial(_basic, _T.GOOD, *_GOOD_SUBDIR)
_good_timestamp = partial(_basic, _T.GOOD, *(_GOOD_SUBDIR + _TIMESTAMP_SUBDIR))
_bad = partial(_basic, _T.BAD, *_BAD_SUBDIR)
_bad_timestamp = partial(_basic, _T.BAD, *(_BAD_SUBDIR + _TIMESTAMP_SUBDIR))
_bad_utf8 = partial(_basic, _T.BAD, *(_BAD_SUBDIR + _UTF8_SUBDIR))


@parametrize(*chain(
    _good(),
    _good_timestamp(),
    _bad(),
    _bad_timestamp(),
    _bad_utf8(),
))
def test_all(p):
    p.test_thunk()

