package com.github.simond.snowflake_csv_inserter

import java.sql.{Connection, DriverManager, ResultSetMetaData, SQLException}

import org.apache.commons.csv.CSVRecord
import java.util.Properties

import scala.util.{Failure, Success, Try}
import org.slf4j.LoggerFactory

case class NoTableFoundException(reason: String) extends SQLException(reason)

object SnowflakeWrapper {
  private val logger = LoggerFactory.getLogger(getClass)

  def getConnection(username: String, password: String, accountName: String, regionId: String,
                    database: Option[String], schema: Option[String], warehouse: Option[String]): Try[Connection] = {

    val properties = new Properties()
    val driver = "net.snowflake.client.jdbc.SnowflakeDriver"
    val url = s"jdbc:snowflake://$accountName.$regionId.snowflakecomputing.com"

    properties.put("user", username)
    properties.put("password", password)
    properties.put("db", database.getOrElse(""))
    properties.put("schema", schema.getOrElse(""))
    properties.put("warehouse", warehouse.getOrElse(""))

    logger.info("Connecting to Snowflake..")
    println("Connecting to Snowflake...")
    Try({
      Class.forName(driver)
      DriverManager.getConnection(url, properties)
    })
  }

  def writeBatches(records: Iterator[CSVRecord], conn: Connection, table: String, batchSize: Int): Try[Int] = {
    val colTypes = getTableColumnTypes(conn, table)
    var batchNumber = 0
    var rowsInserted = 0

    colTypes.map(colTypes => {
      val sql = s"insert into ${table} values (${colTypes.map(x => "?").mkString(", ")})"
      val ps = conn.prepareStatement(sql)

      var batchRowsInserted = 0
      while (records.hasNext) {
        val record = records.next()
        ps.clearParameters()
        colTypes.foreach { colType =>
          ps.setObject(colType._1, record.get(colType._1 - 1), colType._2)
        }
        ps.addBatch()
        batchRowsInserted += 1
        if (batchRowsInserted == batchSize || !records.hasNext) {
          batchNumber += 1
          logger.info(s"Writing batch ${batchNumber} with ${batchRowsInserted} records to Snowflake...")
          println(s"Writing batch ${batchNumber} with ${batchRowsInserted} records to Snowflake...")
          ps.executeBatch()
          rowsInserted += batchRowsInserted
          logger.info(s"Done writing batch ${batchNumber}. ${rowsInserted} written so far")
          println(s"Done writing batch ${batchNumber}. ${rowsInserted} written so far")
          batchRowsInserted = 0
        }
      }
      rowsInserted
    })
  }

  private def getTableColumnTypes(conn: Connection, tableName: String): Try[List[(Int, Int)]] = {
    val metadata: Try[ResultSetMetaData] = Try {
      val statement = conn.createStatement()
      statement.executeQuery(s"select * from $tableName where 1=0").getMetaData
    } match {
      case Success(value) => Success(value)
      case Failure(e: SQLException) if e.getErrorCode == 2003 =>
        Failure(NoTableFoundException(s"Couldn't find table $tableName. Do you have access to it?"))
      case Failure(e) => Failure(e)
    }

    metadata.map(metadata =>
      for (
        i <- { 1 to metadata.getColumnCount }.toList;
        colType = metadata.getColumnType(i)
      ) yield {
        (i, colType)
      }
    )
  }
}
