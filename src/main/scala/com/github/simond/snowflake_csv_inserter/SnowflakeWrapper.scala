package com.github.simond.snowflake_csv_inserter

import java.sql.{Connection, DriverManager, ResultSetMetaData, SQLException}
import java.util.Properties
import scala.util.{Failure, Success, Try}
import org.slf4j.LoggerFactory
import CustomExceptions.{NoTableFoundException, ColumnCountMismatch}


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

  case class ColumnMetadata(columnLocation: Int, columnName: String, columnDataType: Int)

  def writeBatches[T: Gettable](records: Iterator[T], conn: Connection, table: String, batchSize: Int): Try[Int] = {
    val getter = implicitly[Gettable[T]]
    val colTypes = getTableColumnTypes(conn, table)

    colTypes.map(colTypes => {
      var batchNumber = 0
      var rowsInserted = 0
      var batchRowsInserted = 0

      val sql = s"insert into ${table} values (${colTypes.map(x => "?").mkString(", ")})"
      val ps = conn.prepareStatement(sql)

      while (records.hasNext) {
        val record = records.next()
        if (getter.getFieldCount(record) != colTypes.size) {
          return Failure(ColumnCountMismatch(s"Number of fields or columns in the records provided " +
            s"(${getter.getFieldCount(record)}) do not  match the number of columns in the database " +
            s"table (${colTypes.size})"))
        }

        ps.clearParameters()
        colTypes.foreach { colType =>
          ps.setObject(colType.columnLocation, getter.getField(record, colType.columnLocation - 1), colType.columnDataType)
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

  private def getTableColumnTypes(conn: Connection, tableName: String): Try[List[ColumnMetadata]] = {
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
        i <- (1 to metadata.getColumnCount).toList;
        colType = ColumnMetadata(i, metadata.getColumnName(i), metadata.getColumnType(i))
      ) yield colType
    )
  }
}
