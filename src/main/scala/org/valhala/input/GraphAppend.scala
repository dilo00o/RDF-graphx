package org.valhala.input

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Grow an existing graph
 * @author Mircea
*/

object GraphAppend {


  val sc = new SparkContext(new SparkConf().setAppName("Graph creation from rdf"))

  /**
    * Save the new graph to hdfs
    *
    * @param graph newly fusioned graph
    * @param fn    location where to save the file
    */
  def saveGraph(graph: Graph[_, _], fn: String): Unit = {
    graph.vertices.saveAsObjectFile(fn + "vertices") // toplevel name
    graph.edges.saveAsObjectFile(fn + "edges")
  }

  def main(args: Array[String]) {
    if (args.length == 3 && args(0) != args(3))
      GraphCreate.save(union(args(0), args(1)), args(2))
    else println("You must provide source of existing graph, location of the rdf file to be added, output location for the union")
  }


  /**
    * Fusion existing graph structured data with new n-triple data
    *
    * @param fo path+file of the graph stored on hdfs
    * @param fn path+filename of the n-triple file
    * @return graph (fusion of all)
    */
  def union(fo: String, fn: String): Graph[_, _] = {
    val l = loadFromHDFS(fo)
    val (vtx, edge) = loadNewData(fn)
    Graph(l._1.union(vtx), l._2.union(edge))
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

  /**
    * Load n-triple file into vertex and edges
    *
    * @param fn location of the n-triple file
    * @return tuple (RDD[vertex] RDD[edges])
    */
  def loadNewData(fn: String): (RDD[(VertexId, String)], RDD[Edge[String]]) = {
    val triples_file = sc.textFile(fn)
    val rs = triples_file.map(RDF.parseTriple)
    val vertex = rs.flatMap(x => Seq((x._1.##.toLong, x._1), (x._3.##.toLong, x._3))).distinct()
    val edge_array = rs.map(x => Edge(x._1.##.toLong, x._3.##.toLong, x._2))
    (vertex, edge_array)


  }
}
