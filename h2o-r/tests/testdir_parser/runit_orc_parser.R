setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source("../../scripts/h2o-r-test-setup.R")

# This simple test is used to make sure that orc file parsing works across the REST
# API for R clients.

test.orc_parser <- function(){
  # all orc files that Tom K has found
  allOrcFiles = c("smalldata/parser/orc/TestOrcFile.columnProjection.orc",
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
                  "smalldata/parser/orc/version1999.orc")

  for (temp in 1:length(allOrcFiles)) {
    fIndex = temp-1   # translate from python indexing which starts from 0

    if ((fIndex == 4) || (fIndex == 6) || (fIndex == 18) || (fIndex == 23) || (fIndex == 28))
      next   # do not support metadata from user

    if (fIndex == 31)   # contain only orc header, no column and no row, total file size is 0.
      next

    if (fIndex == 19)   # different column names are used between stripes
      next

    if (fIndex == 26)   # abnormal orc file, no inpsector structure available
      next

    if (fIndex ==30)   # problem getting the right column number and then comparison problem
      next

    if (fIndex == 22)     # problem with BufferedString retrieval for binary, wait for Tomas
      next

    if (fIndex == 17)   # problem with bigint retrieval, wait for Tomas
      next

    if (fIndex == 24)  # do not read user metadata
      next

    if (fIndex == 32)  # no column information, only orc header, badly formed file
      next

#    randomInt = sample(1:10,1)
#    if (randomInt > 3)
#      next
    
    h2oFrame = h2o.importFile(locate(allOrcFiles[temp]))
  }

}

doTest("Orc parser Test", test.orc_parser )
