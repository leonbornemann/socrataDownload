package de.hpi.dataset_versioning.crawl

import scala.io.Source

object URLAnalysis extends App {

  val a = Source.fromFile("/home/leon/data/dataset_versioning/socrata/sampleToCrawl2.txt")
    .getLines()
  val b = a.map(url => {
    val tokens = url.split("/")
    tokens(tokens.size-1)
  })
  println(b.toSet.size)

}
