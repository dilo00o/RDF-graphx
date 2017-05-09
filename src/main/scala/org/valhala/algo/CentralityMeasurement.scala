package org.valhala.algo

import org.apache.spark.graphx.{Edge, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * This module computes the closeness centrality measurement
  * @author Mircea
  */
object CentralityMeasurement {
  val sc = new SparkContext(new SparkConf().setAppName("Closeness centrality calculator"))
  val options = mutable.HashMap.empty[String, ArrayBuffer[String]]

  def main(args: Array[String]): Unit = {
    val (vtx, edges) = loadFromHDFS(args(0))
    val graph = Graph(vtx, edges)
    //    val output = ClosenessCentrality.run(graph).vertices.map(a => (a._1, a._2)).collect().toSet[(VertexId, Double)]
    val output = ClosenessCentrality.run(graph).vertices
    //    val pw = new PrintWriter(new File(args(1)))
    //    pw.println("Vertex id  ||     Centrality score")
    //    output.foreach(x => pw.println(s"${x._1}      ||     ${x._2}"))
    //    pw.close()
    //
    //    sc.save
    output.saveAsTextFile(args(1))
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


