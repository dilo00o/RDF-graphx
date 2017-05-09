package org.valhala.output

import org.apache.spark.graphx.Graph
import org.valhala.input.GraphAppend
/**
  * Module providing exportation from a graph to a visualizable format
  * @author Mircea
  */
object Visualizer {
  def main(args: Array[String]) {
    if (args.length == 3) {
      val d = GraphAppend.loadFromHDFS(args(0))
      val g = Graph(d._1, d._2)
      args(2) match {
        case "cytoscape" => VizUtils.saveToCytoscapeFormat(g, args(1))
        case "d3" => VizUtils.saveToD3JsonFormat(g, args(1))
        case "dot" => VizUtils.saveToDotFormat(g, args(1))
        case _ =>
          println("Format not supported")
          System.exit(1)
      }

    } else println("Parameters: <input graph location> <output file+path> <export format>")
  }
}
