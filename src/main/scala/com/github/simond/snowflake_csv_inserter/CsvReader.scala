package com.github.simond.snowflake_csv_inserter

import java.io.{Closeable, File, FileNotFoundException}
import java.nio.charset.Charset
import scala.jdk.CollectionConverters._
import org.apache.commons.csv.{CSVFormat, CSVParser, CSVRecord}
import org.slf4j.LoggerFactory
import scala.util.{Failure, Success, Try}
import CustomExceptions.NoCSVFileFoundException
import java.util.Properties


class CsvReader private(properties: Properties, fileLocation: String) extends Closeable {
  private val delimiter = properties.getProperty("field_delimiter").charAt(0)
  private val nullString = properties.getProperty("null_if").replace("\\", "\\\\")
  private val escapeChar = properties.getProperty("escape").replace("\\", "\\\\").charAt(0)
  private val quoteChar = properties.getProperty("field_optionally_enclosed_by").charAt(0)
  private val skipHeader = properties.getProperty("skip_header")
  private val recordDelimiter = properties.getProperty("record_delimiter")
  private val logger = LoggerFactory.getLogger(getClass)
  private var format = CSVFormat.newFormat(delimiter)
    .withIgnoreEmptyLines(false)
    .withNullString(nullString)
    .withQuote(quoteChar)
    .withRecordSeparator(recordDelimiter)
    .withEscape(escapeChar)

  if(skipHeader == "true"){
    format = format.withFirstRecordAsHeader()
  }

  private val parser = CSVParser.parse(new File(fileLocation), Charset.forName("UTF-8"), format)

  val iterator: Iterator[CSVRecord] = parser.iterator().asScala

  def close(): Unit = {
    logger.debug(s"Closing file ${fileLocation}")
    parser.close()
  }
}

object CsvReader {
  def apply(properties: Properties, fileLocation: String): Try[CsvReader] = {
    Try(new CsvReader(properties, fileLocation)) match {
      case Success(e) => Success(e)
      case Failure(e: FileNotFoundException) =>
        Failure(NoCSVFileFoundException(s"Couldn't find CSV file $fileLocation"))
      case Failure(e) => Failure(e)
    }
  }
}
