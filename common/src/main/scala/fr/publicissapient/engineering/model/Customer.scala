package fr.publicissapient.engineering.model

import fr.publicissapient.engineering.utils.SparkSessionProvider
import org.apache.spark.sql.{DataFrame, SaveMode}

case class Customer(id: Int,
                    firstName: String,
                    lastName: String,
                    age: Int,
                    deleted: Boolean)

object Customer extends SparkSessionProvider with App {

  import spark.implicits._

  val customers: DataFrame = List(
    Customer(1, "Grace", "Hopper", 50, deleted = false),
    Customer(2, "Alan", "Turing", 38, deleted = false),
    Customer(3, "Margaret", "Hamilton", 41, deleted = false)
  ).toDF()

  val newCustomers: DataFrame = List(
    Customer(2, "Alan", "Turing", 38, deleted = true),
    Customer(3, "Margaret", "Hamilton", 42, deleted = false),
    Customer(4, "Linus", "Torvalds", 23, deleted = false)
  ).toDF()

  val finalCustomers: DataFrame = List(
    Customer(1, "Grace", "Hopper", 50, deleted = false),
    Customer(3, "Margaret", "Hamilton", 42, deleted = false),
    Customer(4, "Linus", "Torvalds", 23, deleted = false)
  ).toDF()

  customers.show()
  newCustomers.show()
  finalCustomers.show()

}