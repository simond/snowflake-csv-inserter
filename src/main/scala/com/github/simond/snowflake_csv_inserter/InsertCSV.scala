package com.github.simond.snowflake_csv_inserter

import java.util.Properties

import org.rogach.scallop.ScallopConf

import scala.util.Using
import scala.util.{Failure, Success}
import org.slf4j.LoggerFactory
import org.rogach.scallop._
import java.io.FileReader

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val snowflakeConfigFile: ScallopOption[String] = opt[String](required = true, descr = "The location of the Snowflake configuration file")
  val fileLocation: ScallopOption[String] = opt[String](required = true, descr = "The location of the CSV file")
  val batchSize: ScallopOption[Int] = opt[Int](required = false, default = Some(10000), descr = "The number of rows to insert into snowflake per batch, default is 10000")
  verify()
}

object InsertCSV extends App {
  private val logger = LoggerFactory.getLogger(getClass)
  val conf = new Conf(args)
  val snowflakeConfigFile = new FileReader(conf.snowflakeConfigFile())
  val prop = new Properties
  val insertTemplate = "insert into CUSTOMER values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  val colTypes = List("int","string","int","int","int","int","int","string","string","string","string","int","int",
    "int","string","string","string","string")

  prop.load(snowflakeConfigFile)

  val connection = SnowflakeJdbcWrapper.getConnection(
    prop.getProperty("username"),
    prop.getProperty("password"),
    prop.getProperty("account"),
    prop.getProperty("region"),
    database = prop.getProperty("db"),
    schema = prop.getProperty("schema"),
    warehouse = prop.getProperty("warehouse")
  )

  val v = connection match {
    case Success(connection) =>
      Using.Manager { use =>
        val conn = use(connection)
        val csvIterator = use(CsvReader(',', conf.fileLocation())).iterator
        time {
          SnowflakeJdbcWrapper.writeBatches(csvIterator, conn, insertTemplate, colTypes, conf.batchSize())
        }
      }
    case Failure(e) => logger.error(s"Unable to connect to database: \n$e"); throw e
  }

  v match {
    case Success(e) => logger.info("Done"); println(s"Done ${e}")
    case Failure(e) => logger.error(e.toString); throw e
  }

  def time[R](block: => R): R = {
    val t0: Float = System.nanoTime()
    val result = block
    val t1: Float = System.nanoTime()
    println("Elapsed time: " + ((t1 - t0) / 1000000) + " milliseconds")
    result
  }

}
