package org.valhala.util

import java.io._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.Graph
import org.valhala.input.GraphAppend
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.Path

/**
  * Utility module providing subgraph extraction && Graph analytics
  * @author Mircea
  */
object Utils {
  val sc = new SparkContext(new SparkConf().setAppName("GraphUtils"))

  def main(args: Array[String]) {
    if (args.length == 2) {
      val d = GraphAppend.loadFromHDFS(args(0))
      val g = Graph(d._1, d._2)
      val conf = new Configuration()
      val fs = FileSystem.get(conf)
      val out = new BufferedWriter(new OutputStreamWriter(fs.append(new Path(args(1)))))
      out.write(s"Number of vertices: ${g.vertices.count}")
      out.write(s"Number of edges: ${g.edges.count}")
      out.write(s"Number of triplets: ${g.triplets.count}")
    } else println("Parameters: <input graph location> <output file+path> <export format>")
  }

  /**
    * Return a subgraph matching edge property
    *
    * @param graph        input graph
    * @param edgeProperty property of the edges
    * @return subgraph with edges matching the property
    */
  def subgraphByEdgePredicate(graph: Graph[String, String], edgeProperty: String): Graph[String, String] = {
    graph.subgraph(epred = e => e.attr == edgeProperty)
  }

  /**
    * Return a subgraph matching destination vertex property
    *
    * @param graph               input graph
    * @param destinationProperty vertex property
    * @return subgraph with destination destination vertex attr matching @param destinationProperty
    */
  def subgraphByDestinationPropertyPredicate(graph: Graph[String, String], destinationProperty: String): Graph[String, String] = {
    graph.subgraph(epred = e => e.dstAttr == destinationProperty)
  }

  /**
    * Return a subgraph matching source vertex property
    *
    * @param graph          input graph
    * @param sourceProperty vertex property
    * @return subgraph with source vertex attr matching @param sourceProperty
    */
  def subgraphBySourcePropertyPredicate(graph: Graph[String, String], sourceProperty: String): Graph[String, String] = {
    graph.subgraph(epred = e => e.srcAttr == sourceProperty)
  }
}
