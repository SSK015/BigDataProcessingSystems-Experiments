import org.apache.spark.{SparkConf, SparkContext}

<!-- import org.apache.spark.{SparkConf, SparkContext} -->

val sparkConf = new SparkConf().setAppName("MyApp").setMaster("local")
val sc = SparkContext.getOrCreate(sparkConf)

val lines = sc.textFile("hdfs://localhost:9000/user/xyw/test.txt")

lines.count()