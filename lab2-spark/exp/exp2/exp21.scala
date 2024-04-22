import org.apache.spark.{SparkConf, SparkContext}

<!-- import org.apache.spark.{SparkConf, SparkContext} -->

val sparkConf = new SparkConf().setAppName("MyApp").setMaster("local")
val sc = SparkContext.getOrCreate(sparkConf)
val textFile = sc.textFile("/data/ywxia/BigDataProcessingSystems-Experiments/lab2-spark/exp/exp2/README.md")
textFile.count()

val linesCountWithHadoop = textFile.filter(line => line.contains("Hadoop")).count()