package com.github.simond.snowflake_csv_inserter

import java.sql.{Connection, DatabaseMetaData, DriverManager}

import org.apache.commons.csv.CSVRecord
import java.util.Properties

import org.slf4j.LoggerFactory
import net.snowflake.client.jdbc.SnowflakeType

import scala.util.Try

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

  def writeBatches(records: Iterator[CSVRecord], conn: Connection, sql: String, colTypes: List[String],
                   batchSize: Int): Int = {
    val ps = conn.prepareStatement(sql)
    var batchNumber = 0
    var rowsInserted = 0
    val batched = records.grouped(batchSize)

    batched.foreach(batch => {
      var batchRows = 0
      batchNumber += 1

      batch.foreach(record => {
        var colIndex = 0
        ps.clearParameters()
        colTypes.foreach { colType =>
          val targetType = if (record.get(colIndex) == null) "NULL" else colType
          targetType.toUpperCase() match {
            case "INT" => ps.setInt(colIndex + 1, record.get(colIndex).toInt)
            case "NULL" => ps.setNull(colIndex + 1, 0)
            case _ => ps.setString(colIndex + 1, record.get(colIndex))
          }
          colIndex += 1
        }
        batchRows += 1;
        ps.addBatch()
      })
      logger.info(s"Writing batch ${batchNumber} with ${batchRows} records to Snowflake...")
      println(s"Writing batch ${batchNumber} with ${batchRows} records to Snowflake...")
      ps.executeBatch()
      rowsInserted += batchRows
      logger.info(s"Done writing batch ${batchNumber}. ${rowsInserted} written so far...")
      println(s"Done writing batch ${batchNumber}")
    })
    rowsInserted
  }

  def getTableColumnTypes(conn: Connection, tableName: String, databaseName: Option[String] = None,
                          schemaName: Option[String] = None, ignoreQuotedCase: Boolean = true): Option[Map[Int, String]] = {
    val toUpper = (x: String) => if(ignoreQuotedCase) x.toUpperCase else x
    val db = databaseName.map(toUpper).getOrElse(conn.getCatalog)
    val schema = schemaName.map(toUpper).getOrElse(conn.getSchema)
    val table = toUpper(tableName)

    val tableColumns = conn.getMetaData.getColumns(db, schema, table, null)
    var columnTypes: Map[Int, String] = Map()
    while(tableColumns.next()) {
      columnTypes += tableColumns.getInt("ORDINAL_POSITION") -> SnowflakeType.javaTypeToClassName(tableColumns.getInt("DATA_TYPE"))
    }

    if(columnTypes.isEmpty) None else Option(columnTypes)
  }
}
