package tutorial

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCountApp").setMaster("local")
    val sc = new SparkContext(conf)

    val inputFile = sc.textFile("./resources/input.txt")
    /* ----------------- Transformations ----------------- */
    val counts = inputFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_)
    /* ----------------- Action ----------------- */
    counts.saveAsTextFile("./output/")
  }
}

