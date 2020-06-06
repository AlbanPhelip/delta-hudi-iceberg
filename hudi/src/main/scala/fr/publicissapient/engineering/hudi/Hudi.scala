package fr.publicissapient.engineering.hudi

import fr.publicissapient.engineering.model.People._
import fr.publicissapient.engineering.utils.ExtensionMethodsUtils._
import fr.publicissapient.engineering.utils.FileUtils
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object Hudi extends App {

  val tablePath = s"${args.head}/hudi"
  FileUtils.delete(tablePath)

  // Initialization
  people.write.
    options(getQuickstartWriteConfigs).
    option(PRECOMBINE_FIELD_OPT_KEY, "id").
    option(RECORDKEY_FIELD_OPT_KEY, "id").
    option(TABLE_NAME, "hudi").
    hudi(tablePath)

  // Update and Insert
  newPeople.filter(col("deleted") === "false").
    write.
    options(getQuickstartWriteConfigs).
    option(OPERATION_OPT_KEY, "upsert").
    option(PRECOMBINE_FIELD_OPT_KEY, "id").
    option(RECORDKEY_FIELD_OPT_KEY, "id").
    option(TABLE_NAME, "hudi").
    mode(SaveMode.Append).
    hudi(tablePath)

  // Delete
  newPeople.filter(col("deleted") === "true").
    write.
    options(getQuickstartWriteConfigs).
    option(OPERATION_OPT_KEY, "delete").
    option(PRECOMBINE_FIELD_OPT_KEY, "id").
    option(RECORDKEY_FIELD_OPT_KEY, "id").
    option(TABLE_NAME, "hudi").
    mode(SaveMode.Append).
    hudi(tablePath)

  spark.read.hudi(s"$tablePath/*").show(truncate = false)

}