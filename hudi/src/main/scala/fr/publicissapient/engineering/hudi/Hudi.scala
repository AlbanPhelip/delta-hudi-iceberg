package fr.publicissapient.engineering.hudi

import fr.publicissapient.engineering.model.Customer._
import fr.publicissapient.engineering.utils.ExtensionMethodsUtils._
import fr.publicissapient.engineering.utils.FileUtils
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object Hudi extends App {

  val tableName = "hudi"
  val rootPath = s"${args.head}/hudi"
  FileUtils.delete(rootPath)

  customers.write.
    options(getQuickstartWriteConfigs).
    option(PRECOMBINE_FIELD_OPT_KEY, "id").
    option(RECORDKEY_FIELD_OPT_KEY, "id").
    option(TABLE_NAME, tableName).
    mode(SaveMode.Overwrite).
    hudi(rootPath)

  println("Before upsert")
  spark.read.hudi(s"$rootPath/*").show(truncate = false)

  newCustomers.filter(col("deleted") === "false").
    write.
    options(getQuickstartWriteConfigs).
    option(PRECOMBINE_FIELD_OPT_KEY, "id").
    option(RECORDKEY_FIELD_OPT_KEY, "id").
    option(TABLE_NAME, tableName).
    mode(SaveMode.Append).
    hudi(rootPath)

  newCustomers.filter(col("deleted") === "true").
    write.
    options(getQuickstartWriteConfigs).
    option(OPERATION_OPT_KEY, "delete").
    option(PRECOMBINE_FIELD_OPT_KEY, "id").
    option(RECORDKEY_FIELD_OPT_KEY, "id").
    option(TABLE_NAME, tableName).
    mode(SaveMode.Append).
    hudi(rootPath)

  println("After upsert")
  spark.read.hudi(s"$rootPath/*").show(truncate = false)

}