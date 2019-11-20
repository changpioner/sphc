package com.nam.test.sphc

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types.StructType

class CustomDataSourceV2ReaderFactory(requiredSchema: StructType, pushed: Array[Filter], options: Map[String, String]) extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] = {
    new CustomDataReader(requiredSchema,pushed,options)
  }

}
