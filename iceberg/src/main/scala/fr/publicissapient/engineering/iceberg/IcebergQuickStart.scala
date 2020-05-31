package fr.publicissapient.engineering.iceberg


import fr.publicissapient.engineering.utils.{FileUtils, SparkSessionProvider}

import fr.publicissapient.engineering.model.Person
import org.apache.spark.sql.DataFrame
import fr.publicissapient.engineering.utils.ExtensionMethodsUtils._
import org.apache.iceberg.{BaseTable, PartitionSpec, Table}
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.hive.HiveCatalog
import org.apache.iceberg.spark.SparkSchemaUtil


object IcebergQuickStart extends App with SparkSessionProvider {

  import spark.implicits._

  val rootPath = args.head

  FileUtils.delete(rootPath)

  val df1: DataFrame = List(
    Person("Toto", 21, "2020-06-09"),
    Person("Titi", 30, "2019-06-09")
  ).toDF()

  val schema = SparkSchemaUtil.convert(df1.schema)
  val tables = new HadoopTables(spark.sessionState.newHadoopConf())

  val spec = PartitionSpec.builderFor(schema)
    .identity("name")
    .build()

  private val tablePath = "iceberg-quick-start"

  val table = tables.create(schema, spec, s"file://$rootPath/$tablePath")

  df1.write
    .mode("append").iceberg(s"file://$rootPath/$tablePath")

  // read the table
  spark.read
    .iceberg(s"$rootPath/$tablePath")
    .show()

  spark.read
    .parquet(s"$rootPath/$tablePath")
    .show()

}
