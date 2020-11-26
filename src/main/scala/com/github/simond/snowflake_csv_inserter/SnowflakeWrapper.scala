package com.github.simond.snowflake_csv_inserter

import java.sql.{Connection, DriverManager, SQLException}

import org.apache.commons.csv.CSVRecord
import java.util.Properties

import scala.util.{Failure, Success, Try}
import org.slf4j.LoggerFactory

case class NoColumnsFoundException(reason: String) extends SQLException(reason)

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

    logger.info("Connecting to Snowflake")
    Try({
      Class.forName(driver)
      DriverManager.getConnection(url, properties)
    })
  }

  def writeBatches(records: Iterator[CSVRecord], conn: Connection, table: String, batchSize: Int): Try[Int] = {
    var batchNumber = 0
    var rowsInserted = 0
    val batched = records.grouped(batchSize)
    logger.debug("SnowflakeWrapper.scala: Batches grouped")
    val colTypes = getTableColumnTypes(conn, table)

    colTypes.map(colTypes => {
      val sql = s"insert into ${table} values (${colTypes.map(x => "?").mkString(", ")})"
      val ps = conn.prepareStatement(sql)

      batched.foreach(batch => {
        var batchRows = 0
        batchNumber += 1

        batch.foreach(record => {
          ps.clearParameters()
          colTypes.foreach { colType =>
            ps.setObject(colType._1, record.get(colType._1 - 1), colType._2)
          }
          batchRows += 1;
          ps.addBatch()
        })
        logger.info(s"Writing batch ${batchNumber} with ${batchRows} records to Snowflake...")
        ps.executeBatch()
        rowsInserted += batchRows
        logger.info(s"Done writing batch ${batchNumber}. ${rowsInserted} written so far...")
      })
      rowsInserted
    })
  }

  private def getTableColumnTypes(conn: Connection, tableName: String, databaseName: Option[String] = None,
                                  schemaName: Option[String] = None, ignoreQuotedCase: Boolean = true): Try[Map[Int, Int]] = {

    val toUpper = (x: String) => if (ignoreQuotedCase) x.toUpperCase else x
    val db = databaseName.map(toUpper).getOrElse(conn.getCatalog)
    val schema = schemaName.map(toUpper).getOrElse(conn.getSchema)
    val table = toUpper(tableName)

    // Get columns from database (Doesn't return an exception if the table doesn't exist)
    val tableColumns = Try(conn.getMetaData.getColumns(db, schema, table, null))

    tableColumns.flatMap(cols => {
      var columnTypes: Map[Int, Int] = Map()
      while (cols.next()) {
        columnTypes += cols.getInt("ORDINAL_POSITION") -> cols.getInt("DATA_TYPE")
      }
      if(columnTypes.nonEmpty){
        Success(columnTypes)
      } else {
        Failure(NoColumnsFoundException(s"No columns in table $table or the table was not found"))
      }
    })
  }
}
