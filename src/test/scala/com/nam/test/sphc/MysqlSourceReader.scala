package com.nam.test.sphc

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD}
import org.apache.spark.sql.sources.{EqualTo, Filter, GreaterThan, IsNotNull}
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

class MysqlSourceReader(options: Map[String, String])
  extends DataSourceReader with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {
  val supportedFilters: ArrayBuffer[Filter] = ArrayBuffer[Filter]()
  var requiredSchema: StructType = {
    val jdbcOptions = new JDBCOptions(options)
    JDBCRDD.resolveTable(jdbcOptions)
    //StructType.fromDDL("`s` INT,`t` LONG,`v` DOUBLE")
  }

  override def readSchema(): StructType = {
    //val jdbcOptions = new JDBCOptions(options)
    //JDBCRDD.resolveTable(jdbcOptions)
    requiredSchema
  }

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    import collection.JavaConverters._
    Seq(
      new CustomDataSourceV2ReaderFactory(requiredSchema, pushedFilters(), options).asInstanceOf[DataReaderFactory[Row]]
    ).asJava
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (supported, unsupported) = filters.partition(support)
    supportedFilters ++= supported
    unsupported
  }

  def support(filter: Filter): Boolean = {
    filter match {
      case _: EqualTo => true
      case _: GreaterThan => true
      case _: IsNotNull => true
      case _ => false
    }
  }

  override def pushedFilters(): Array[Filter] = supportedFilters.toArray

  override def pruneColumns(requiredSchema: StructType): Unit = this.requiredSchema = requiredSchema
}
