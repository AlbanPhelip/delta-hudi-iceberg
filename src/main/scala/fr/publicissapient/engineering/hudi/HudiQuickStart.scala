package fr.publicissapient.engineering.hudi

import fr.publicissapient.engineering.utils.{FileUtils, SparkSessionProvider}

import fr.publicissapient.engineering.model.Person
import fr.publicissapient.engineering.utils.ExtensionMethodsUtils._
import org.apache.hudi.QuickstartUtils._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.{DataFrame, SaveMode}



object HudiQuickStart extends App with SparkSessionProvider {


  import spark.implicits._

  val tableName = "hudi_trips_cow"
  val rootPath = args.head

  FileUtils.delete(rootPath)

  val df1: DataFrame = List(
    Person("Toto", 21, "2020-06-09"),
    Person("Titi", 30, "2019-06-09")
  ).toDF()

  def writeData(df: DataFrame, saveMode: SaveMode): Unit = {
    df.write.
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "date").
      option(RECORDKEY_FIELD_OPT_KEY, "name").
      //option(PARTITIONPATH_FIELD_OPT_KEY, "date").
      option(TABLE_NAME, tableName).
      mode(saveMode).
      hudi(s"$rootPath/hudi-quick-start")
  }

  def printData(): Unit = {
    spark.read
      .hudi(s"$rootPath/hudi-quick-start/*")
      .show(truncate = false)
  }

  writeData(df1, Overwrite)
  printData()


  val df2: DataFrame = List(
    Person("Toto", 22, "2020-06-10"),
    Person("Tata", 30, "2020-06-10")
  ).toDF()

  writeData(df2, Append)
  printData()

}
