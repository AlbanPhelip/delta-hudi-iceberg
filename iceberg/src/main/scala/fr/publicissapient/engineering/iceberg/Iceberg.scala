package fr.publicissapient.engineering.iceberg

import fr.publicissapient.engineering.model.Customer._
import fr.publicissapient.engineering.utils.ExtensionMethodsUtils._
import fr.publicissapient.engineering.utils.FileUtils
import org.apache.iceberg.{PartitionSpec, Table}
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.spark.SparkSchemaUtil


object Iceberg extends App {

  val rootPath = s"${args.head}/iceberg"
  FileUtils.delete(rootPath)

  val schema = SparkSchemaUtil.convert(customers.schema)
  val tables = new HadoopTables(spark.sessionState.newHadoopConf())

  val spec = PartitionSpec.builderFor(schema)
    //.identity("id")
    .build()

  val table: Table = tables.create(schema, spec, s"file://$rootPath/")

  customers.write
    .mode("append").iceberg(s"file://$rootPath/")

  val firstTimestamp = System.currentTimeMillis()
  Thread.sleep(1000)

  println("Before overwrite")
  spark.read.iceberg(rootPath).show()

  newCustomers.write
    .mode("overwrite").iceberg(s"file://$rootPath/")

  println("After overwrite")
  spark.read.iceberg(rootPath).show()

  println("Time travel")
  spark.read
    .option("as-of-timestamp", s"$firstTimestamp")
    .iceberg(rootPath)
    .show()
}
