package org.valhala.algo

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
 * Benchmarking module for centrality computation
*/
object CentralityMeasurementBenchmark {
  val sc = new SparkContext(new SparkConf().setAppName("Closeness centrality calculator"))
  val options = mutable.HashMap.empty[String, ArrayBuffer[String]]

  def main(args: Array[String]): Unit = {
    val (vtx, edges) = loadFromHDFS(args(0))
    val graph = Graph(vtx, edges)
    val output = ClosenessCentrality.run(graph).vertices.map(a => (a._1, a._2)).collect().toSet[(VertexId, Double)]
    output
  }


  /**
    * Loads existing graph (from hdfs)
    *
    * @param fv path+filename of the file to be loaded (vertex and edges files are handled at the same time
    * @return tuple of RDD[vertex] and RDD[edges]
    */

  def loadFromHDFS(fv: String): (RDD[(VertexId, String)], RDD[Edge[String]]) = {
    (sc.objectFile(fv + "vertices"), sc.objectFile(fv + "edges"))
  }

}
