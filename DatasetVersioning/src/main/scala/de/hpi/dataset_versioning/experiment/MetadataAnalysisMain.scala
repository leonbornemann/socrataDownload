package de.hpi.dataset_versioning.experiment

import java.io.{File, StringReader}

import com.google.gson.JsonParser
import com.google.gson.stream.JsonReader
import de.hpi.dataset_versioning.data.metadata.DatasetMetadata

import scala.collection.mutable
import scala.io.Source

object MetadataAnalysisMain extends App {

  val metadataDir = new File(args(0))
  val days = metadataDir.listFiles()
    .filter(_.isDirectory)
  val idToFilename = mutable.HashMap[String,mutable.HashSet[String]]()
  days.foreach(d => {
    val metadataFiles = d
      .listFiles()
    var filenames = mutable.HashSet[String]()
    var totalSize = 0
    metadataFiles.foreach(f => {
      val a = Source.fromFile(f).mkString
      val reader = new JsonReader(new StringReader(a))
      val parser = new JsonParser();
      val curArray = parser.parse(reader).getAsJsonArray
      totalSize +=curArray.size()
      assert(curArray.isJsonArray)
      (0 until curArray.size()).foreach(i => {
        val datasetMetadata = DatasetMetadata.fromJsonString(curArray.get(i).toString)
        filenames += datasetMetadata.resource.name
        val fnames = idToFilename.getOrElseUpdate(datasetMetadata.resource.id,mutable.HashSet())
        fnames += datasetMetadata.resource.name
      })
    })
    println(s"Day: ${d.getName}")
    println(s"Uniqueness of filenames: ${filenames.size /totalSize.toDouble}")
  })
  println(idToFilename.size)
  println(idToFilename.filter(_._2.size>1).size)
}
