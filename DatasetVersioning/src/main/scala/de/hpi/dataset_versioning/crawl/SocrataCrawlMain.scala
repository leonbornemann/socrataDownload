package de.hpi.dataset_versioning.crawl

import java.io.{File, PrintWriter, StringReader}
import java.net.URL
import java.time.{LocalDate, LocalDateTime}

import com.google.gson.{JsonArray, JsonObject, JsonParser}
import com.google.gson.stream.JsonReader

import scala.collection.mutable
import scala.io.Source

object SocrataCrawlMain extends App {
  val metadataResultDir = args(0) + "/" + LocalDate.now() + "/"
  val urlFile = args(1) + "/" + LocalDate.now() + "/"
  new File(metadataResultDir).mkdirs()
  new File(urlFile).mkdirs()
  new SocrataMetadataCrawler(metadataResultDir).crawl(urlFile)
}
