package fr.publicissapient.engineering.delta

import fr.publicissapient.engineering.model.Customer._
import fr.publicissapient.engineering.utils.ExtensionMethodsUtils._
import fr.publicissapient.engineering.utils.FileUtils
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col

object DeltaLake extends App {

  val rootPath = args.head
  val personPath = s"$rootPath/delta"
  FileUtils.delete(personPath)

  customers.coalesce(3).write.mode(SaveMode.Overwrite).delta(personPath)

  val deltaCustomer = DeltaTable.forPath(personPath)

  println("Before upsert")
  println("Old data")
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

  println("After upsert")
  spark.read.delta(personPath).show()

  println("History")
  deltaCustomer.history.show(truncate = false)

}
