package org.itc.com
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object scalademo {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","firstdemo")

    val sc = new SparkContext (master= "local",appName= "aaa")

    //val rdd1 = sc.textFile(path = "D:\\spark_code\\sparkdemo\\input\\data.txt")
    // path \\ or / works fine
    val rdd1 = sc.textFile(path = "UKUSMarHDFS/vanathi/xyz13.txt")
    //val rdd1 = sc.textFile(args(0))


    val rdd2 = rdd1.flatMap(x => x.split(" "))

    val rdd3 = rdd2.map(x => (x.toLowerCase(),1))

    //val rdd4 = rdd3.reduceByKey((x,y)=> x+y).sortByKey() //sort by key
    // Tuple(abc,2) for tuple index starts from 1 (1 - abc, 2- 2)
    // sortBy(_._1) sort by first column , sortBy(_._2) sort by second column
    val rdd4 = rdd3.reduceByKey((x,y)=> x+y).sortBy(_._2) // sort by value


    // val rdd5 = rdd4.sortByKey()
    // outOfMemory Exception
    // val result = rdd4.collect()

    rdd4.collect().foreach(println)  }
}
