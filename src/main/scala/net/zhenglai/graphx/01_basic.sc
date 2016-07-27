import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader

// remove this line from spark-shelli
val sc = new SparkContext()

// load file of edge-list format
// Graph[Int, Int]
val graph = GraphLoader.edgeListFile(sc, "src/main/resources/cit-HepTh.txt")

// find papers most cited by others
graph.inDegrees.reduce((a, b) ⇒ if (a._2 > b._2) a else b)
// inDegrees creates an RDD of VertexID/in-degree pairs
// res2: (org.apache.spark.graphx.VertexId, Int) = (9711200,2414)

graph.vertices.take(10)
// VertextId -> Int

// graphs are immutable
val v = graph.pageRank(0.001).vertices
v.take(10)

// select the most influential paper id, at least according to the PageRank
v.reduce((a, b) ⇒ if (a._2 > b._2) a else b)

val rdd = sc.makeRDD(Array(1, 2, 3))
val rdd2 = rdd.map(2 * _).cache // map transformation is queued up, no computation takes place
//  actually, a DAg of pending operations is constructed
val result = rdd2.reduce(_ + _) // reduce is one action, so map starts execute
rdd2.count // oh, start from map again by default!!, so we added cache!! so count doesn't need to re-execute the whole pipeline

// persist is more flexible, see code plz.




