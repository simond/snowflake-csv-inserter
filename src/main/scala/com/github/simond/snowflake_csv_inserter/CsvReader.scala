package com.github.simond.snowflake_csv_inserter

import java.io.{Closeable, File, FileNotFoundException}
import java.nio.charset.Charset

import scala.jdk.CollectionConverters._
import org.apache.commons.csv.{CSVFormat, CSVParser, CSVRecord}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

case class NoCSVFileFoundException(reason: String) extends FileNotFoundException(reason)

class CsvReader private(delimiter: Char, fileLocation: String) extends Closeable {
  private val logger = LoggerFactory.getLogger(getClass)
  private val format = CSVFormat.newFormat(delimiter)
    .withIgnoreEmptyLines(false)
    .withNullString("\\N")
    .withQuote('"')
    .withRecordSeparator('\n')
    .withEscape('\\')

  private val parser = CSVParser.parse(new File(fileLocation), Charset.forName("UTF-8"), format)

  val iterator: Iterator[CSVRecord] = parser.iterator().asScala

  def close(): Unit = {
    logger.debug(s"Closing file ${fileLocation}")
    parser.close()
  }
}

object CsvReader {
  def apply(delimiter: Char, fileLocation: String): Try[CsvReader] = {
    Try(new CsvReader(delimiter, fileLocation)) match {
      case Success(e) => Success(e)
      case Failure(e: FileNotFoundException) =>
        Failure(NoCSVFileFoundException(s"Couldn't find CSV file $fileLocation"))
      case Failure(e) =>
        throw e;
    }
  }
}
