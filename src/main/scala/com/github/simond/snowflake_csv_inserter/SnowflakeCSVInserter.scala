package com.github.simond.snowflake_csv_inserter

import java.util.Properties

import org.rogach.scallop.ScallopConf

import scala.util.{Failure, Success, Using}
import org.slf4j.LoggerFactory
import org.rogach.scallop._
import java.io.{FileNotFoundException, FileReader, PrintWriter, StringWriter}
import java.sql.SQLException

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val snowflakeConfigFile: ScallopOption[String] = opt[String](required = true, descr = "The location of the Snowflake configuration file")
  val fileLocation: ScallopOption[String] = opt[String](required = true, descr = "The location of the CSV file")
  val targetTable: ScallopOption[String] = opt[String](required = true, descr = "The table to insert into")
  val batchSize: ScallopOption[Int] = opt[Int](required = false, default = Some(10000), descr = "The number of rows to insert into snowflake per batch, default is 10000")
  verify()
}

object SnowflakeCSVInserter extends App {
  private val logger = LoggerFactory.getLogger(getClass)
  val conf = new Conf(args)
  val prop = new Properties

  // Try to read the properties file
  try {
    prop.load(new FileReader(conf.snowflakeConfigFile()))
  } catch {
    case e: FileNotFoundException =>
      logger.error(s"Unable to find Snowflake config file ${conf.snowflakeConfigFile()}")
      println(s"Unable to find Snowflake config file ${conf.snowflakeConfigFile()}")
      System.exit(1)
  }

  val connection = SnowflakeWrapper.getConnection(
    prop.getProperty("username"),
    prop.getProperty("password"),
    prop.getProperty("account"),
    prop.getProperty("region"),
    database = Option(prop.getProperty("db")),
    schema = Option(prop.getProperty("schema")),
    warehouse = Option(prop.getProperty("warehouse"))
  )

  val (rowsWritten: Int, milliseconds: Float) = connection.flatMap(connection =>
    Using.Manager({ use =>
      val conn = use(connection)
      val csvIterator = use(CsvReader(',', conf.fileLocation()).get).iterator
      time {
        SnowflakeWrapper.writeBatches(csvIterator, conn, conf.targetTable(), conf.batchSize())
      }
    })
  ) match {
    case Success((Success(rowsWritten), milliseconds)) => (rowsWritten, milliseconds)
    case Success((Failure(e: NoTableFoundException), _)) =>
      println(e.reason)
      logStackTraceAndExit(e, logger.error)
    case Success((Failure(e), _)) => throw e;
    case Failure(e: NoCSVFileFoundException) =>
      println(e.reason)
      logStackTraceAndExit(e, logger.error)
    case Failure(e) =>
      throw e;
  }

  println(s"$rowsWritten rows written in $milliseconds milliseconds. That's ${rowsWritten / milliseconds} rows per millisecond")

  def time[R](block: => R): (R, Float) = {
    val t0: Float = System.nanoTime()
    val result = block
    val t1: Float = System.nanoTime()
    val elapsedTime = ((t1 - t0) / 1000000)
    (result, elapsedTime)
  }

  def logStackTraceAndExit(exception: Throwable, mode: String => Unit): Unit = {
    val sw = new StringWriter
    exception.printStackTrace(new PrintWriter(sw))
    mode(sw.toString)
    System.exit(1)
  }

}
