package fr.publicissapient.engineering.delta

import fr.publicissapient.engineering.model.Customer._
import fr.publicissapient.engineering.utils.{FileUtils, SparkSessionProvider}
import fr.publicissapient.engineering.utils.ExtensionMethodsUtils._
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SaveMode

object DeltaMerge extends App {

  val rootPath = args.head
  val personPath = s"$rootPath/delta-upsert"
  FileUtils.delete(personPath)


  customers.coalesce(3).write.mode(SaveMode.Overwrite).delta(personPath)

  val deltaCustomer = DeltaTable.forPath(personPath)

  println("Before merge")
  deltaCustomer.toDF.show()

  println("New data")
  newCustomers.show()

  val oldTableName = "old-customer"
  val newTableName = "new-customer"

  deltaCustomer.as(oldTableName)
    .merge(newCustomers.as(newTableName),  col(s"$oldTableName.id") ===  col(s"$newTableName.id"))
    .whenMatched(col(s"$newTableName.deleted") === true)
    .delete()
    .whenMatched()
    .updateAll()
    .whenNotMatched()
    .insertAll()
    .execute()

  println("After merge")
  spark.read.delta(personPath).show()

  println("History")
  deltaCustomer.history.show(truncate = false)

}
