package org.valhala.input

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Module providing graph creation from n-triple file
  * @author Mircea
  */
object GraphCreate {
  val sc = new SparkContext(new SparkConf().setAppName("Graph creation from rdf"))

  def main(args: Array[String]): Unit = {
    if (args.length == 2 ) {
      val triples_file = sc.textFile(args(0)).cache
      val rs = triples_file.map(RDF.parseTriple)
      val edge_array = rs.map(x => Edge(x._1.##.toLong, x._3.##.toLong, x._2))
      val vertices_array = rs.flatMap(x => Seq((x._1.##.toLong, x._1), (x._3.##.toLong, x._3))).distinct()
      val graph = Graph(vertices_array, edge_array) // create the graph
      save(graph, args(1))
    } else {
      println("You must provide the file you wish to convert to a graph")
    }

  }

  /**
    * Save the graph to hdfs
    * it will not use a standard file handler
    *
    * @param graph Graph to save
    * @param fn    path to location where to save
    */
  def save(graph: Graph[_, _], fn: String = ""): Unit = {
    graph.edges.saveAsObjectFile(fn + "edges")
    graph.vertices.saveAsObjectFile(fn + "vertices")

  }
}
