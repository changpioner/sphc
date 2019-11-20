package com.nam.test.sphc

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}


class DefaultSource extends DataSourceV2 with ReadSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    new MysqlSourceReader(
      Map("url" -> options.get("url").get(),
        "user" -> options.get("user").get(),
        "password" -> options.get("password").get(),
        "driver" -> options.get("driver").get(),
        "dbtable" -> options.get("dbtable").get()
      )
    )
  }

}
