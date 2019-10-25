package de.hpi.dataset_versioning.crawl

import java.io.PrintWriter

import scala.io.Source
import scala.util.Random

object SocrataURLanalysisMain extends App {
  val urls = Source.fromFile("/home/leon/data/dataset_versioning/socrata/datasetURLs.txt").getLines().toSeq
  val urlsDistinct = Source.fromFile("/home/leon/data/dataset_versioning/socrata/datasetURLs.txt").getLines().toSeq
  val lineCount = Source.fromFile("/home/leon/data/dataset_versioning/socrata/linecount").getLines().toSeq
    .map(_.split("\\s+"))
    .filter(_.size==3)
    .map(_(1).toInt)
    .groupBy(i => i)
    .mapValues(_.size)
  lineCount.toSeq.sorted.foreach(println)


  val withTopLevelDomain=urlsDistinct.groupBy(url => {
    val domains = url.split("https://")(1).split("/resource/")(0).split("\\.").reverse.toSeq
    if (domains.size>=2) domains.slice(0,2) else Seq(domains(0))
  })
  val urlCount = withTopLevelDomain
    .mapValues(_.size)
  urlCount.foreach(println(_))


  //multiURLOccurrenceAnalysis
  //writeDistinctURLS

  private def writeDistinctURLS = {
    val urlsNoDuplicates = new PrintWriter("/home/leon/data/dataset_versioning/socrata/datasetURLs_unique.txt")
    urls.toSet.foreach((url:String) => urlsNoDuplicates.println(url))
    urlsNoDuplicates.close()
  }


  private def multiURLOccurrenceAnalysis = {
    val (singleSize, multiSize) = urls.zipWithIndex
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .partition(_._2.size == 1)
    println(singleSize.size)
    println(multiSize.size)
    multiSize.foreach(println(_))
    multiSize.mapValues(_.max)
      .values.toSeq.sorted
      .foreach(println(_))
    Random.shuffle((singleSize.keySet ++ multiSize.keySet)
      .toSeq)
      .take(400)
      .foreach(println(_))
    println((singleSize.keySet ++ multiSize.keySet).size)
  }

}
