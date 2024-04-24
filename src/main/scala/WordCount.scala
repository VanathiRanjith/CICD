
import org.apache.spark.sql.SparkSession
object WordCount {
    def main(args: Array[String]): Unit = {
      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("WordCount")
        .getOrCreate()

      // Import implicits for Encoders
      import spark.implicits._

      // Read the input file
      val inputFile = args(0)
      val inputDS = spark.read.textFile(inputFile)

      // Perform word count
      val wordCounts = inputDS
        .flatMap(_.split("\\s+"))
        .groupByKey(_.toLowerCase)
        .count()

      // Output the word counts
      wordCounts.show()

      // Stop the SparkSession
      spark.stop()
    }
  }
