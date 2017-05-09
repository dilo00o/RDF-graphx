package org.valhala.output

import org.valhala.util.Utils
import org.apache.spark.graphx.Graph
import org.valhala.input.GraphAppend

/**
  * Module providing export from a subgraph identified by an attribute to a visualizable format
  * @author Mircea
  */
object VisualiserSubgraph {
  def main(args: Array[String]) {
    if (args.length == 5) {
      val d = GraphAppend.loadFromHDFS(args(0))
      var g: Graph[String, String] = null // replace by a beter forward decl
      args(3) match {
        case "0" => g = Utils.subgraphByEdgePredicate(Graph(d._1, d._2), args(4))
        case "1" => g = Utils.subgraphBySourcePropertyPredicate(Graph(d._1, d._2), args(4))
        case "2" => g = Utils.subgraphByDestinationPropertyPredicate(Graph(d._1, d._2), args(4))
        case _ =>
          println("Subgraph option error")
          System.exit(1)
      }

      args(2) match {
        case "cytoscape" => VizUtils.saveToCytoscapeFormat(g, args(1))
        case "d3" => VizUtils.saveToD3JsonFormat(g, args(1))
        case "dot" => VizUtils.saveToDotFormat(g, args(1))
        case _ =>
          println("Format not supported")
          System.exit(1)
      }
    }
    else println("Parameters: <input graph location> <output file+path> <export format>")
  }
}