package de.hpi.dataset_versioning.crawl

import java.io.File

import scala.io.Source

object URLAnalysis extends App {

  val fromServer = "/home/leon/data/dataset_versioning/socrata/fromServer/urls"
  val updated = "/home/leon/data/dataset_versioning/socrata/urls/2019-10-30"
  val urlAfterUpdate = new File(updated).listFiles().flatMap(Source.fromFile(_).getLines().toSet).toSet
  println(urlAfterUpdate.size)
  val allInServer = new File(fromServer).listFiles().flatMap(f => f.listFiles().flatMap(Source.fromFile(_).getLines().toSet)).toSet
  val individualSizes = new File(fromServer).listFiles().map(f => (f.getName,f.listFiles().flatMap(Source.fromFile(_).getLines().toSet).size)).toSet
  println(individualSizes)
  println(allInServer.size)
  println(urlAfterUpdate.diff(allInServer).size)
}
