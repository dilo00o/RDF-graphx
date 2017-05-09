package org.valhala.algo

import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


object ClosenessCentrality {

  /**
    * Calculate the closeness centrality of each vertex in the input graph.
    *
    * @tparam VD the vertex attribute type (discarded in the computation)
    * @tparam ED the edge attribute type (preserved in the computation)
    * @param graph the graph for which to compute the closeness centrality
    * @return a graph with vertex attributes containing its closeness centrality
    */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Double, ED] = {
    Graph(ShortestPaths.run(graph, graph.vertices.map { vx => vx._1 }.collect())
      .vertices.map {
      vx => (vx._1, {
        val dx = 1.0 / vx._2.values.sum
        if (dx.isNaN | dx.isNegInfinity | dx.isPosInfinity) 0.0 else dx
      })
    }: RDD[(VertexId, Double)], graph.edges)
  }


//  /**
//    *
//    */
//  def runWithOnlyVertices[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): RDD[(Long, (String, Double))]:RDD[(Long, (String, Double))] = {
//    ShortestPaths.run(graph, graph.vertices.map { vx => vx._1 }.collect())
//  }

  implicit def iterableWithAvg[T: Numeric](data: Iterable[T]): Object {def avg: Double} = new {
    def avg = average(data)
  }

  def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
    num.toDouble(ts.sum) / ts.size
  }
}
