package com.github.simond.snowflake_csv_inserter

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}
import org.apache.commons.csv.CSVRecord
import java.util.Properties
import org.slf4j.LoggerFactory
import scala.util.Try

class SnowflakeJdbcWrapper {
}

object SnowflakeJdbcWrapper {
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

    logger.info("Connecting to Snowflake")
    Try({
      Class.forName(driver)
      DriverManager.getConnection(url, properties)
    })
  }

  def writeBatches(records: Iterator[CSVRecord], conn: Connection, sql: String, colTypes: List[String],
                   batchSize: Int): Unit = {
    val ps = conn.prepareStatement(sql)
    var rowsRead = 0
    var batchNumber = 0

    while (records.hasNext) {
      val line = records.next()
      var colIndex = 0

      ps.clearParameters()
      rowsRead += 1
      for(colType <- colTypes) {
        val tt: String = if (line.get(colIndex) == null) "NULL" else colType
        Try(
          tt.toUpperCase() match {
            case "INT" => ps.setInt(colIndex+1, line.get(colIndex).toInt)
            case "NULL" => ps.setNull(colIndex+1, 0)
            case _ => ps.setString(colIndex+1, line.get(colIndex))
          }
        ).getOrElse({
          logger.warn(s"Unable to convert ${line.get(colIndex)} at param index ${colIndex+1} to ${tt}, inserting NULL instead")
          ps.setNull(colIndex+1, 0)
        })
        colIndex += 1
      }
      ps.addBatch()

      if (rowsRead == batchSize.toInt || !records.hasNext) {
        batchNumber += 1
        logger.info(s"Writing batch ${batchNumber} with ${rowsRead} records to Snowflake...")
        println(s"Writing batch ${batchNumber} with ${rowsRead} records to Snowflake...")
        ps.executeBatch()
        logger.info(s"Done writing batch ${batchNumber}")
        println(s"Done writing batch ${batchNumber}")
        rowsRead = 0
      }
    }
  }
}
