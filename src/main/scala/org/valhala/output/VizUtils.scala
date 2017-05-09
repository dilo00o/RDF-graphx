package org.valhala.output

import java.io._

import org.apache.spark.graphx._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.Path


/**
 * Vizualization utility module
 * @author Mircea
*/
object VizUtils {

  def saveToCytoscapeFormat(graph: Graph[_, _], outputFileName: String, directed: Boolean = true): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val out = new BufferedWriter(new OutputStreamWriter(fs.append(new Path(outputFileName))))
    out.write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>")
    out.write(toXGMML(graph, directed).toString)
    out.close
  }

  /** *
    * Outputs XGMML structured output
    *
    * @param graph    graph to visualise
    * @param directed type of graph (directed undirected)
    * @param id       id of the vertex
    * @param label    Label of the graph (visualisable in cytoscape)
    * @return
    */
  def toXGMML(graph: Graph[_, _], directed: Boolean = true, id: Long = 0L, label: String = "Cytoscape output") = {
    <graph id={id.toString} label={label} directed={if (directed) 1.toString else 0.toString} cy:documentVersion="3.0" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:cy="http://www.cytoscape.org" xmlns="http://www.cs.rpi.edu/XGMML">
      {graph.vertices.collect.map(vertex => <node id={vertex._1.toString} label={vertex._2.toString}>
      <att name="val" type="string" value={vertex._2.toString}/>
    </node>)}{graph.edges.collect.map(edge => <edge label={edge.attr.toString} source={edge.srcId.toString} target={edge.dstId.toString}></edge>)}
    </graph>
  }

  /**
    * Wrapper for d3 json formatted graph
    *
    * @param graph          Graph to visualise
    * @param outputFileName path+filename for the output
    */
  def saveToD3JsonFormat(graph: Graph[_, _], outputFileName: String): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val out = new BufferedWriter(new OutputStreamWriter(fs.append(new Path(outputFileName))))
    out.write(j3JsonFormatter(graph))
    out.close
  }

  /**
    * Formats the graph to j3 - json
    *
    * @param graph the graph to visualise
    * @return j3 json representation of the graph
    */
  def j3JsonFormatter(graph: Graph[_, _]): String = {
    val verts = graph.vertices.collect
    val edges = graph.edges.collect
    val nmap = verts.zipWithIndex.map(v => (v._1._1.toLong, v._2)).toMap
    val vtxt = verts.map(v =>
      "{\"val\":\"" + v._2.toString + "\"}").mkString(",\n")
    val etxt = edges.map(e =>
      "{\"source\":" + nmap(e.srcId).toString +
        ",\"target\":" + nmap(e.dstId).toString +
        ",\"value\":" + e.attr.toString + "}").mkString(",\n")

    "{ \"nodes\":[\n" + vtxt + "\n],\"links\":[" + etxt + "]\n}"
  }

  /**
    * Wrapper for dot formatter
    *
    * @param g              the graph to visualise
    * @param outputFileName path+filename for the output
    */
  def saveToDotFormat(g: Graph[String, String], outputFileName: String): Unit = {

    toDot(g, outputFileName)
  }

  /**
    * Formats the graph to dot representation
    *
    * @param graph       the graph to visualise
    * @param printWriter handle for printwriter
    */
  def toDot(graph: Graph[_, _], outputFileName: String): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val out = new BufferedWriter(new OutputStreamWriter(fs.append(new Path(outputFileName))))
    out.write("digraph Graph{")
    for (triplet <- graph.triplets.collect) {
      out.write(triplet.srcId + " [ label = \"" + triplet.srcAttr + "\"];")
      out.write(triplet.dstId + " [ label = \"" + triplet.dstAttr + "\"];")
      out.write(triplet.srcId + " -> " + triplet.dstId + " [ label = \"" + triplet.srcAttr + "\"];")
    }
    out.write("}")
    out.close
  }
}