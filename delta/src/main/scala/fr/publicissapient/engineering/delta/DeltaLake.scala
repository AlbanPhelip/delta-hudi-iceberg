package fr.publicissapient.engineering.delta

import fr.publicissapient.engineering.model.People._
import fr.publicissapient.engineering.utils.ExtensionMethodsUtils._
import fr.publicissapient.engineering.utils.FileUtils
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.col

object DeltaLake extends App {

  val tablePath = s"${args.head}/delta"
  FileUtils.delete(tablePath)

  // Initialization
  people.write.delta(tablePath)

  val deltaPeople = DeltaTable.forPath(tablePath)
  deltaPeople.as("current-people")
    .merge(newPeople.as("new-people"),  col("current-people.id") ===  col("new-people.id"))
    .whenMatched(col("new-people.deleted") === true)
    .delete()
    .whenMatched()
    .updateAll()
    .whenNotMatched()
    .insertAll()
    .execute()

  spark.read.delta(tablePath).show()

}
