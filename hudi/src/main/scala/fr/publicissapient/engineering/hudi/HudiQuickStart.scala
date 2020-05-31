package fr.publicissapient.engineering.hudi

import fr.publicissapient.engineering.model.Customer._
import fr.publicissapient.engineering.utils.ExtensionMethodsUtils._
import fr.publicissapient.engineering.utils.FileUtils
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

object HudiQuickStart extends App {

  val tableName = "hudi_trips_cow"
  val rootPath = s"${args.head}/hudi-upsert"

  FileUtils.delete(rootPath)

  def writeData(df: DataFrame, saveMode: SaveMode, mode: String = ""): Unit = {
    df.write.
      options(getQuickstartWriteConfigs).
      option(OPERATION_OPT_KEY, mode).
      option(PRECOMBINE_FIELD_OPT_KEY, "id").
      option(RECORDKEY_FIELD_OPT_KEY, "id").
      option(TABLE_NAME, tableName).
      mode(saveMode).
      hudi(rootPath)
  }

  def printData(): Unit = {
    spark.read
      .hudi(s"$rootPath/*")
      .show(truncate = false)
  }

  writeData(customers, Overwrite)
  printData()

  writeData(newCustomers.filter(col("deleted") === "false"), Append)
  writeData(newCustomers.filter(col("deleted") === "true"), Append, "delete")
  printData()

}