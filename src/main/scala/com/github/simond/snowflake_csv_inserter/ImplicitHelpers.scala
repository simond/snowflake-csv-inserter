package com.github.simond.snowflake_csv_inserter

import org.apache.commons.csv.CSVRecord

object ImplicitHelpers {
  implicit object CSVRecordsGettable extends Gettable[CSVRecord] {
    def get(csvRecord: CSVRecord, i: Int): String = {
      csvRecord.get(i)
    }
  }
}
