package com.sparkTutorial.pairRdd.join

import org.apache.spark.{SparkConf, SparkContext}

object JoinOperations {

    def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("JoinOperations").setMaster("local[1]")
        val sc = new SparkContext(conf)

        val ages = sc.parallelize(List(("Tom", 29),("John", 22)))
        val addresses = sc.parallelize(List(("James", "USA"), ("John", "UK")))

        val join = ages.join(addresses)
        for (word <- join) println(word)

       // val leftOuterJoin = ages.leftOuterJoin(addresses)
     //   for (word <- leftOuterJoin) println(word)

      //  val rightOuterJoin = ages.rightOuterJoin(addresses)
      //  for (word <- rightOuterJoin) println(word)

      //  val fullOuterJoin = ages.fullOuterJoin(addresses)
      //  for (word <- fullOuterJoin) println(word)
    }
}
