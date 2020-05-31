package fr.publicissapient.engineering.utils

import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Delta Lake XKE")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

}
