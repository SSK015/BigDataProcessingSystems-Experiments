import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.lib.StronglyConnectedComponents
import org.apache.spark.{SparkConf, SparkContext}

object GraphSCC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GraphSCC").setMaster("local")
    val sc = new SparkContext(conf)

    val vertices: RDD[(VertexId, String)] = sc.textFile("input/graphx-wiki-vertices.txt")
      .map { line =>
        val fields = line.split('\t')
        (fields(0).toLong, fields(1))
      }
    val edges: RDD[Edge[Int]] = sc.textFile("input/graphx-wiki-edges.txt")
      .map { line =>
        val fields = line.split('\t')
        Edge(fields(0).toLong, fields(1).toLong, 0)
      }

    val graph: Graph[String, Int] = Graph(vertices, edges)

    val sccResult: Graph[VertexId, Int] = graph.stronglyConnectedComponents(100)

    val sccCount: Long = sccResult.vertices.map { case (_, componentId) => componentId }.distinct().count()

    val group = sccResult.vertices.map{
      case (verticeId, minId) => (minId, verticeId)
    }.groupByKey()

    group.foreach(println)

    println(s"Number of strongly connected components: $sccCount")

    sc.stop()
  }
}