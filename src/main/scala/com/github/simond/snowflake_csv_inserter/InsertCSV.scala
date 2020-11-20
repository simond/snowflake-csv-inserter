package com.github.simond.snowflake_csv_inserter

import java.util.Properties

import org.rogach.scallop.ScallopConf

import scala.util.{Try, Using}
import org.slf4j.LoggerFactory
import org.rogach.scallop._
import java.io.{FileNotFoundException, FileReader}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val snowflakeConfigFile: ScallopOption[String] = opt[String](required = true, descr = "The location of the Snowflake configuration file")
  val fileLocation: ScallopOption[String] = opt[String](required = true, descr = "The location of the CSV file")
  val batchSize: ScallopOption[Int] = opt[Int](required = false, default = Some(10000), descr = "The number of rows to insert into snowflake per batch, default is 10000")
  verify()
}

object InsertCSV extends App {
  private val logger = LoggerFactory.getLogger(getClass)
  val conf = new Conf(args)
  val prop = new Properties
  val insertTemplate = "insert into test_db.public.customer values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  val colTypes = List("int", "string", "int", "int", "int", "int", "int", "string", "string", "string", "string", "int", "int",
    "int", "string", "string", "string", "string")

  // Try to read the properties file
  try {
    prop.load(new FileReader(conf.snowflakeConfigFile()))
  } catch {
    case e: FileNotFoundException =>
      logger.error(s"Unable to find Snowflake config file ${conf.snowflakeConfigFile()} \n $e")
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

  val rowsWritten: Try[Int] = connection.flatMap(connection =>
    Using.Manager({ use =>
      val conn = use(connection)
      val csvIterator = use(CsvReader(',', conf.fileLocation())).iterator
      time {
        SnowflakeWrapper.writeBatches(csvIterator, conn, insertTemplate, colTypes, conf.batchSize())
      }
    })
  )

  println(rowsWritten)

  def time[R](block: => R): R = {
    val t0: Float = System.nanoTime()
    val result = block
    val t1: Float = System.nanoTime()
    println("Elapsed time: " + ((t1 - t0) / 1000000) + " milliseconds")
    result
  }

}
