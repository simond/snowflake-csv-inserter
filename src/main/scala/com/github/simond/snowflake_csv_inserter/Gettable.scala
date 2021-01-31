package com.github.simond.snowflake_csv_inserter

import org.apache.commons.csv.CSVRecord

trait Gettable[T] {
  def getField(t: T, index: Int): String

  def getFieldCount(t: T): Int
}

object Gettable {

  implicit object CSVRecordsGettable extends Gettable[CSVRecord] {
    def getField(csvRecord: CSVRecord, i: Int): String = {
      csvRecord.get(i)
    }

    def getFieldCount(csvRecord: CSVRecord): Int = {
      csvRecord.size()
    }
  }

}

