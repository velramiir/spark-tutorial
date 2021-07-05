package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.collect_set

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    import spark.implicits._

    val data = inputs.map(path => spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(path)
    );

//    data.head.crossJoin(data(1)).show;
//    data(0).join(data(1), data(0)("R_COMMENT") === data(1)("N_COMMENT"), joinType = "fullouter").show;
//    println(data.zipWithIndex)

//    println(data.zipWithIndex.flatMap(pair => pair._1.columns.zip(Stream.continually(pair._2))))
//    val allColumns = data.zipWithIndex.flatMap(pair => pair._1.columns.zip(Stream.continually(pair._2)))

    val cells = data
      .map(df =>
        df.flatMap(row => row
          .toSeq
          .map(_.toString)
          .zipWithIndex
          .map(value => (value._1, row.schema.fieldNames(value._2))))
      )
      .reduce(_.union(_))
      .groupBy("_1")
      .agg(collect_set("_2").as("attributes"))

    cells.show()

    cells
      .select("attributes")
      .flatMap(row => row
        .getSeq[String](0)
        .map(attribute => (attribute, row.getSeq[String](0).filterNot(col => col == attribute))))
      .groupByKey(row => row._1)
      .mapGroups((key, iterator) => iterator.reduce((attribute_set, aggregate) => attribute_set.intersect(aggregate) ))



  //  data(0)
  //    .select(data(0)("R_NAME"))
  //    .join(
  //      data(0)
  //      .select(data(0)("R_REGIONKEY")),
  //      data(0)("R_NAME") === data(0)("R_REGIONKEY"),
  //      joinType = "fullouter")
  //    .show
  }
}


/*
for table1 in data:
  for table2 in data:
    for col1 in table1:
      for col2 in table2:
        output = table1.join(table2, col1 == col2)
        output.map(col1 != null ? table1)

for row in data:
  tuples = row.map(makeTuple)


Value1 | R_REGIONKEY
Value2 | R_REGIONKEY
Value3 | R_REGIONKEY
Value4 | R_NAME
Value3 | R_NAME

-->

R_NAME | R_REGIONKEY

R_REGIONKEY | NAME | N_REGIONKEY

Value 1 | Null | Value 1 --> {R_REGIONKEY, N_REGIONKEY}
Value 2 | Null | Null --> R_REGIONKEY
Null    | Null | Value 3 --> N_REGIONKEY


Null | Value 4 | Null
 */