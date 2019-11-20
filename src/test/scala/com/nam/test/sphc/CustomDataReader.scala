package com.nam.test.sphc

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.sql.Row
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.sources.v2.reader.DataReader
import org.apache.spark.sql.types._


class CustomDataReader(requiredSchema: StructType, pushed: Array[Filter], options: Map[String, String]) extends DataReader[Row] {
  val initSQL: String = {
    val selected = if (requiredSchema.isEmpty) "1" else requiredSchema.fieldNames.mkString(",")
    if (pushed.nonEmpty) {
      val dialect = JdbcDialects.get(options("url"))
      val filter = pushed.map {
        case EqualTo(attr, value) => s"${dialect.quoteIdentifier(attr)} = ${dialect.compileValue(value)}}"
      }.mkString(" AND ")
      s"SELECT $selected FROM ${options("dbtable")} WHERE $filter"
    }
    else
      s"SELECT $selected FROM ${options("dbtable")}"
  }


  val rs: ResultSet = {
    val conn = DriverManager.getConnection(options("url"), options("user"), options("password"))
    val stmt = conn.prepareStatement(initSQL, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    stmt.setFetchSize(1000)
    stmt.executeQuery()
  }

  override def next(): Boolean = {
    rs.next()

  }

  override def get(): Row = {

    Row(getSeq(requiredSchema, pushed, options): _*)
  }

  override def close(): Unit = {
    rs.close()
    println("close source")
  }


  def getSeq(requiredSchema: StructType, pushed: Array[Filter], options: Map[String, String]): Seq[Any] = {
    requiredSchema.fields.zipWithIndex.map {
      element =>
        element._1.dataType match {
          case IntegerType => rs.getInt(element._2 + 1)
          case DoubleType => rs.getDouble(element._2 + 1)
          case LongType => rs.getLong(element._2 + 1)
          case StringType => rs.getString(element._2 + 1)
          case TimestampType => rs.getTimestamp(element._2 + 1)
          case DateType => rs.getDate(element._2 + 1)
          case _: DecimalType =>
            val d = rs.getBigDecimal(element._2 + 1)
            Decimal(d, d.precision(), d.scale())
        }
    }.toSeq

  }
}
