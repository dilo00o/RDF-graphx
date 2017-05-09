package org.valhala.input

import java.io.InputStream

import org.apache.jena.riot.{Lang, RiotReader}

/**
  * Module providing general RDF processing
  * @author Mircea
  */
object RDF {


  /**
    * Parse line as triple
    *
    * @param line in file to be processed
    * @return
    */
  def parseTriple(line: String): (String, String, String) = {
    val trip = RiotReader.createIteratorTriples(new StringInputStream(line), Lang.NTRIPLES, "").next
    (trip.getSubject.toString,
      trip.getPredicate.getLocalName.toString,
      if (trip.getObject.isLiteral) trip.getObject.getLiteralValue.toString else trip.getObject.toString)
  }

  /**
    * Parses a quad formatted file
    *
    * @param line in file to be processed
    */
  def parseQuad(line: String) = {
    try {
      val quad = RiotReader.createIteratorQuads(new StringInputStream(line), Lang.NQUADS, "").next
      val sub = quad.getSubject.toString
      val pred = quad.getPredicate.toString
      val obj = if (quad.getObject.isLiteral) quad.getObject.getLiteralValue.toString else quad.getObject.toString
      val graph = quad.getGraph.toString
      (sub, pred, obj, graph, true, "")
    } catch {
      case e: Throwable => {
        ("sub", "pred", "obj", line, false, e.getMessage)
      }
    }
  }


  /**
    * Customised string input ( to work on hadoop)
    *
    * @param s string to convert to stream
    */
  class StringInputStream(s: String) extends InputStream {
    private val bytes = s.getBytes

    private var pos = 0

    override def read(): Int = if (pos >= bytes.length) {
      -1
    } else {
      val r = bytes(pos)
      pos += 1
      r.toInt
    }
  }

}