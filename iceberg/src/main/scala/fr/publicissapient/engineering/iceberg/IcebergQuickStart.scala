package fr.publicissapient.engineering.iceberg

import fr.publicissapient.engineering.model.Customer._
import fr.publicissapient.engineering.utils.ExtensionMethodsUtils._
import fr.publicissapient.engineering.utils.FileUtils
import org.apache.iceberg.{PartitionSpec, Table}
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.spark.SparkSchemaUtil


object IcebergQuickStart extends App {

  val rootPath = s"${args.head}/iceberg-upsert"
  FileUtils.delete(rootPath)

  val schema = SparkSchemaUtil.convert(customers.schema)
  val tables = new HadoopTables(spark.sessionState.newHadoopConf())

  val spec = PartitionSpec.builderFor(schema)
    .identity("id")
    .build()


  val table: Table = tables.create(schema, spec, s"file://$rootPath/")

  table.newDelete().

  customers.write
    .mode("append").iceberg(s"file://$rootPath/")

  // read the table
  println("Before merge")
  spark.read
    .iceberg(rootPath)
    .show()

  println("Read Parquet")
  spark.read
    .parquet(rootPath)
    .show()

}
