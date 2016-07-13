from builtins import str
import sys
sys.path.insert(1,"../../")
import h2o
from tests import pyunit_utils
from random import randint

"""
This test takes all orc files collected by Tom K and try to parse them into H2O frames.
Due to test duration limitation, we do not parse all the files.  Instead, we randomly
choose about 30% of the files and parse them into H2O frames.  If all pareses are successful,
the test pass and else it fails.
"""
def orc_parser_test():
    allOrcFiles = ["smalldata/parser/orc/TestOrcFile.columnProjection.orc",
                   "smalldata/parser/orc/bigint_single_col.orc",
                   "smalldata/parser/orc/TestOrcFile.emptyFile.orc",
                   "smalldata/parser/orc/bool_single_col.orc",
                   "smalldata/parser/orc/TestOrcFile.metaData.orc",
                   "smalldata/parser/orc/decimal.orc",
                   "smalldata/parser/orc/TestOrcFile.test1.orc",
                   "smalldata/parser/orc/demo-11-zlib.orc",
                   "smalldata/parser/orc/TestOrcFile.testDate1900.orc",
                   "smalldata/parser/orc/TestOrcFile.testDate1900.orc",
                   "smalldata/parser/orc/demo-12-zlib.orc",
                   "smalldata/parser/orc/TestOrcFile.testDate2038.orc",
                   "smalldata/parser/orc/double_single_col.orc",
                   "smalldata/parser/orc/TestOrcFile.testMemoryManagementV11.orc",
                   "smalldata/parser/orc/float_single_col.orc",
                   "smalldata/parser/orc/TestOrcFile.testMemoryManagementV12.orc",
                   "smalldata/parser/orc/int_single_col.orc",
                   "smalldata/parser/orc/TestOrcFile.testPredicatePushdown.orc",
                   "smalldata/parser/orc/nulls-at-end-snappy.orc",
                   "smalldata/parser/orc/TestOrcFile.testSeek.orc",
                   "smalldata/parser/orc/orc-file-11-format.orc",
                   "smalldata/parser/orc/TestOrcFile.testSnappy.orc",
                   "smalldata/parser/orc/orc_split_elim.orc",
                   "smalldata/parser/orc/TestOrcFile.testStringAndBinaryStatistics.orc",
                   "smalldata/parser/orc/over1k_bloom.orc",
                   "smalldata/parser/orc/TestOrcFile.testStripeLevelStats.orc",
                   "smalldata/parser/orc/smallint_single_col.orc",
                   "smalldata/parser/orc/TestOrcFile.testTimestamp.orc",
                   "smalldata/parser/orc/string_single_col.orc",
                   "smalldata/parser/orc/TestOrcFile.testUnionAndTimestamp.orc",
                   "smalldata/parser/orc/tinyint_single_col.orc",
                   "smalldata/parser/orc/TestOrcFile.testWithoutIndex.orc",
                   "smalldata/parser/orc/version1999.orc"]

    for fIndex in range(len(allOrcFiles)):
        if ((fIndex == 4) or (fIndex == 6) or (fIndex == 18) or (fIndex == 23) or (fIndex == 28)):
            continue   # do not support metadata from user

        if (fIndex == 31):   # contain only orc header, no column and no row, total file size is 0.
            continue

        if (fIndex == 19):   # different column names are used between stripes
            continue

        if (fIndex == 26):   # abnormal orc file, no inpsector structure available
            continue

        if (fIndex ==30):    # problem getting the right column number and then comparison problem
            continue

        if (fIndex == 22):     # problem with BufferedString retrieval for binary, wait for Tomas
            continue

        if (fIndex == 17):   # problem with bigint retrieval, wait for Tomas
            continue

        if (fIndex == 24):  # do not read user metadata
            continue

        if (fIndex == 32):  # no column information, only orc header, badly formed file
            continue

#        randNum = randint(0,9)
        # if (randNum > 3):  # skip test for 70% of the time
        #     continue

        #Test tab seperated files by giving separator argument
        tab_test = h2o.import_file(path=pyunit_utils.locate(allOrcFiles[fIndex]))


if __name__ == "__main__":
    pyunit_utils.standalone_test(orc_parser_test)
else:
    orc_parser_test()






