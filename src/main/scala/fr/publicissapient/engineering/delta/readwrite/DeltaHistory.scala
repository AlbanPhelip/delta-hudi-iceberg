package fr.publicissapient.engineering.delta.readwrite

import fr.publicissapient.engineering.model.Person
import fr.publicissapient.engineering.utils.ExtensionMethodsUtils._
import fr.publicissapient.engineering.utils.{FileUtils, SparkSessionProvider}
import org.apache.spark.sql.{DataFrame, SaveMode}
import io.delta.tables.DeltaTable

object DeltaHistory extends App with SparkSessionProvider {

  import spark.implicits._

  val rootPath = args.head
  val personPath = s"$rootPath/person-history"
  FileUtils.delete(personPath)

  val df: DataFrame = List(
    Person("Toto", 21, "2019-11-05"),
    Person("Titi", 30, "2019-11-05")
  ).toDF()

  df.write.mode(SaveMode.Append).delta(personPath)
  df.write.mode(SaveMode.Append).delta(personPath)
  df.write.mode(SaveMode.Append).delta(personPath)

  val history: DataFrame = DeltaTable.forPath(personPath).history()

  history.show(truncate = false)
}
