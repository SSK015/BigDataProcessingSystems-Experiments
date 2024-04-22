import org.apache.spark.{SparkConf, SparkContext}
import java.io.PrintWriter

<!-- import org.apache.spark.{SparkConf, SparkContext} -->

val sparkConf = new SparkConf().setAppName("MyApp").setMaster("local")
val sc = SparkContext.getOrCreate(sparkConf)

val textFile = sc.textFile("/data/ywxia/BigDataProcessingSystems-Experiments/lab2-spark/exp/exp2/AChristmasCarol_CharlesDickens_Dutch.txt")
val wordCounts = textFile.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey((a,b) => a+b)

val writer = new PrintWriter("/data/ywxia/BigDataProcessingSystems-Experiments/lab2-spark/exp/exp2/output.txt")
wordCounts.collect().foreach(writer.println)
writer.close()

