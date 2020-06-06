package fr.publicissapient.engineering.iceberg

import fr.publicissapient.engineering.model.People._
import fr.publicissapient.engineering.utils.ExtensionMethodsUtils._
import fr.publicissapient.engineering.utils.FileUtils
import org.apache.iceberg.{PartitionSpec, Table}
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.spark.sql.SaveMode


object Iceberg extends App {

  val tablePath = s"${args.head}/iceberg"
  FileUtils.delete(tablePath)

  // Initialization
  val schema = SparkSchemaUtil.convert(people.schema)
  val tables = new HadoopTables(spark.sessionState.newHadoopConf())
  val spec = PartitionSpec.builderFor(schema).build()
  val table: Table = tables.create(schema, spec, s"file://$tablePath/")

  people.write.mode(SaveMode.Append).iceberg(s"file://$tablePath/")

  println("Before overwrite")
  spark.read.iceberg(tablePath).show()

  val timestamp = System.currentTimeMillis()
  newPeople.write.mode(SaveMode.Overwrite).iceberg(s"file://$tablePath/")

  println("After overwrite")
  spark.read.iceberg(tablePath).show()

  println("Time travel")
  spark.read
    .option("as-of-timestamp", s"$timestamp")
    .iceberg(tablePath)
    .show()
}
