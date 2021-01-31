package com.github.simond.snowflake_csv_inserter

import org.apache.commons.csv.CSVRecord

trait Gettable[T] {
  def get(t: T, index: Int): String
}

object Gettable {

  implicit object CSVRecordsGettable extends Gettable[CSVRecord] {
    def get(csvRecord: CSVRecord, i: Int): String = {
      csvRecord.get(i)
    }
  }

}

