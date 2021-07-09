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

    val inclusionLists = data
      .map(df =>
        df.flatMap(row => row
          .toSeq
          .map(_.toString)
          .zip(row.schema.fieldNames)
          )
      )
      .reduce(_.union(_))
      .groupBy("_1")
      .agg(collect_set("_2").as("attributes"))
      .select("attributes")
      .flatMap(row => row
        .getSeq[String](0)
        .map(attribute => (attribute, row.getSeq[String](0).filterNot(_ == attribute))))
      .groupByKey(_._1)
      .mapGroups((key, iterator) => iterator.reduce((attribute_set, aggregate) => (attribute_set._1, attribute_set._2.intersect(aggregate._2)) ))
      .filter(_._2.nonEmpty)
      .sort("_1")
      .collect()

    inclusionLists
      .foreach(inclusionList => println("%1$s < %2$s".format(inclusionList._1, inclusionList._2.mkString(", "))))

  }
}