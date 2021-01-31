package com.github.simond.snowflake_csv_inserter

import java.io.FileNotFoundException
import java.sql.SQLException

object CustomExceptions {
  case class NoCSVFileFoundException(reason: String) extends FileNotFoundException(reason)
  case class NoTableFoundException(reason: String) extends SQLException(reason)
}
