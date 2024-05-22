import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

object GraphDFS {
  def main(args: Array[String]): Unit = {
    // 设置 Spark 环境
    val conf = new SparkConf().setAppName("GraphDFS").setMaster("local")
    val sc = new SparkContext(conf)

    // 读取顶点和边的数据
    val vertices = sc.textFile("input/graphx-wiki-vertices.txt")
      .map { line =>
        val fields = line.split('\t')
        (fields(0).toLong, fields(1))
      }

    // vertices.foreach(println)
    val edges = sc.textFile("input/graphx-wiki-edges.txt")
      .map { line =>
        val fields = line.split('\t')
        Edge(fields(0).toLong, fields(1).toLong, 0)
      }

    // 创建 Graph
    val graph = Graph(vertices, edges).mapVertices((id, label) => label.asInstanceOf[Any])

    // 执行深度优先搜索
    val startVertex = 6598434222544540151L
    val dfsResult = dfs(graph, startVertex)

    // 输出 DFS 结果
    dfsResult.foreach { case (vertex, distance) =>
      val distanceFromStart = if (vertex == startVertex) 0 else distance
      println(s"$vertex\t$distanceFromStart")
    }

    sc.stop()
  }

  def dfs(graph: Graph[Any, Int], startVertex: VertexId): Seq[(VertexId, Int)] = {
    var visited = Set[VertexId]()
    var stack = List[(VertexId, Int)]((startVertex, 0))
    var result = List[(VertexId, Int)]()

    while (stack.nonEmpty) {
      val (currentVertex, distance) = stack.head
      stack = stack.tail

      if (!visited.contains(currentVertex)) {
        visited += currentVertex
        val neighbors = graph.edges.filter(_.srcId == currentVertex).map(_.dstId).collect()
        stack = neighbors.map((_, distance + 1)).toList ::: stack
        result ::= (currentVertex, distance)
      }
    }

    result.reverse
  }
}