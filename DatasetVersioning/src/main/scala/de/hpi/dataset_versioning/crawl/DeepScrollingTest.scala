package de.hpi.dataset_versioning.crawl

import java.io.File
import java.time.LocalDate

import de.hpi.dataset_versioning.crawl.SocrataCrawlMain.args

import scala.io.Source

object DeepScrollingTest extends App {
  //new SocrataMetadataCrawler(null).deepScrollingTest()

  /*val a = new File("/home/leon/data/dataset_versioning/socrata/urls/2019-10-30T12:46:54.576/")
    .listFiles
    .flatMap(f => Source.fromFile(f).getLines().toSeq)
    .toSet
  val b = new File("/home/leon/data/dataset_versioning/socrata/urls/2019-10-30/")
    .listFiles
    .flatMap(f => Source.fromFile(f).getLines().toSeq)
    .toSet
  println(a.size)
  println(b.size)
  println(a.intersect(b).size)*/
  new SocrataMetadataCrawler(null).searchTest()
}
