package fr.publicissapient.engineering.model

import fr.publicissapient.engineering.utils.SparkSessionProvider
import org.apache.spark.sql.DataFrame

case class People(id: Int,
                  firstName: String,
                  lastName: String,
                  age: Int,
                  deleted: Boolean)

object People extends SparkSessionProvider {

  import spark.implicits._

  val people: DataFrame = List(
    People(1, "Grace", "Hopper", 50, deleted = false),
    People(2, "Alan", "Turing", 38, deleted = false),
    People(3, "Margaret", "Hamilton", 41, deleted = false)
  ).toDF()

  val newPeople: DataFrame = List(
    People(2, "Alan", "Turing", 38, deleted = true),
    People(3, "Margaret", "Hamilton", 42, deleted = false),
    People(4, "Linus", "Torvalds", 23, deleted = false)
  ).toDF()

  val finalPeople: DataFrame = List(
    People(1, "Grace", "Hopper", 50, deleted = false),
    People(3, "Margaret", "Hamilton", 42, deleted = false),
    People(4, "Linus", "Torvalds", 23, deleted = false)
  ).toDF()

}