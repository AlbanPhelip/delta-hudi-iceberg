package fr.publicissapient.engineering.utils

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

object ExtensionMethodsUtils {

  implicit class DataFrameWriterOps[T](dfw: DataFrameWriter[T]) {
    def delta(path: String): Unit = {
      dfw.format("delta").save(path)
    }

    def hudi(path: String): Unit = {
      dfw.format("hudi").save(path)
    }

    def iceberg(path: String): Unit = {
      dfw.format("iceberg").save(path)
    }
  }

  implicit class DataFrameReaderOps(dfr: DataFrameReader) {
    def delta(path: String): DataFrame = {
      dfr.format("delta").load(path)
    }

    def hudi(path: String): DataFrame = {
      dfr.format("hudi").load(path)
    }

    def iceberg(path: String): DataFrame = {
      dfr.format("iceberg").load(path)
    }
  }
}
