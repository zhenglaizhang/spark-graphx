import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader

// remove this line from spark-shelli
val sc = new SparkContext()

// load file of edge-list format
val graph = GraphLoader.edgeListFile(sc, "src/main/resources/cit-HepTh.txt")

// Graph[Int, Int]
graph.inDegrees.reduce((a, b) ⇒ if (a._2 > b._2) a else b)
// inDegrees creates an RDD of VertexID/in-degree pairs
// res2: (org.apache.spark.graphx.VertexId, Int) = (9711200,2414)

graph.vertices.take(10)
// VertextId -> Int