package template.spark

import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

final case class Person(id: Int, firstName: String, gender: String, age: Int)

object Main extends InitSpark {
  def main(args: Array[String]) = {
    import spark.implicits._

    println(s"SPARK VERSION = ${spark.version}")

    val sumHundred = spark.range(1, 101).agg(sum("id"))
    println(f"Sum 1 to 100 = $sumHundred")

    val persons = Seq(Person(1, "Jack", "male", 27),
      Person(2, "John", "male", 150),
      Person(3, "Pamela", "female", 40),
      Person(4, "Ann", "female", 27)
    )
    val personsDf = spark.createDataset(persons)
    personsDf.foreach(println(_))

    // Vertex DataFrame
    val v = sqlContext.createDataFrame(List(
      ("a", "Alice", 34),
      ("b", "Bob", 36),
      ("c", "Charlie", 30),
      ("d", "David", 29),
      ("e", "Esther", 32),
      ("f", "Fanny", 36),
      ("g", "Gabby", 60)
    )).toDF("id", "name", "age")
    // Edge DataFrame
    val e = sqlContext.createDataFrame(List(
      ("a", "b", "friend"),
      ("b", "c", "follow"),
      ("c", "b", "follow"),
      ("f", "c", "follow"),
      ("e", "f", "follow"),
      ("e", "d", "friend"),
      ("d", "a", "friend"),
      ("a", "e", "friend")
    )).toDF("src", "dst", "relationship")
    // Create a GraphFrame
    val g = GraphFrame(v, e)
    close()
  }
}
