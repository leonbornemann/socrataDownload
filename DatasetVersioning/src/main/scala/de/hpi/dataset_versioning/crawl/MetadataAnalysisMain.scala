package de.hpi.dataset_versioning.crawl

import java.io.{File, StringReader}

import com.google.gson.JsonParser
import com.google.gson.stream.JsonReader
import de.hpi.dataset_versioning.crawl.data.metadata.DatasetMetadata

import sys.process._
import scala.io.Source

object MetadataAnalysisMain extends App {

  val metadataFiles = new File("/home/leon/data/dataset_versioning/socrata/metadata/2019-10-30/")
      .listFiles()
  val a = Source.fromFile(metadataFiles(0)).mkString
  val reader = new JsonReader(new StringReader(a))
  val parser = new JsonParser();
  var curArray = parser.parse(reader).getAsJsonArray
  assert(curArray.isJsonArray)
  (0 until curArray.size()).foreach(i => {
    val datasetMetadata = DatasetMetadata.fromJsonString(curArray.get(i).toString)
    println(datasetMetadata.permalink)
  })
}
