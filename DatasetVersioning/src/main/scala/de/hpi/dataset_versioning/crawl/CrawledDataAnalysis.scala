package de.hpi.dataset_versioning.crawl

import java.io.{File, FileInputStream}
import java.util.zip.{ZipEntry, ZipInputStream}

import scala.collection.mutable

object CrawledDataAnalysis extends App{
  val source1 = "/home/leon/data/dataset_versioning/socrata/fromServer/2019-10-22/san2/data/change-exploration/socrata/data/2019-10-22/"
  val source2 = "/home/leon/data/dataset_versioning/socrata/fromServer/2019-10-24/san2/data/change-exploration/socrata/data/2019-10-24/"

  def readFileSizes(source1: String) = {
    val zis: ZipInputStream = new ZipInputStream(new FileInputStream(source1))
    var ze: ZipEntry = zis.getNextEntry();
    val map = mutable.HashMap[String,Long]()
    while(ze !=null){
      if(!ze.isDirectory){
        val nameTokens = ze.getName.split("/")
        val name = nameTokens(nameTokens.size-1)
        val size = ze.getSize
        map(name) = size
      }
      ze = zis.getNextEntry
    }
    zis.close()
    map
  }

  def zipVariant() = {
    val directory = "/home/leon/data/dataset_versioning/socrata/fromServer/"
    val files = new File(directory)
      .listFiles()
      .toSeq
      .filter(_.getName.endsWith(".zip"))
      .sorted
    val source1 = files(files.size-2).getAbsolutePath
    val source2 = files(files.size-1).getAbsolutePath

//    val source1 = "/home/leon/data/dataset_versioning/socrata/fromServer/2019-10-22.zip"
//    val source2 = "/home/leon/data/dataset_versioning/socrata/fromServer/2019-10-24.zip"
    val f1Map = readFileSizes(source1)
    val f2Map = readFileSizes(source2)
    val numNewFiles = f2Map.keySet.diff(f1Map.keySet).size
    val numDeletedFiles = f1Map.keySet.diff(f2Map.keySet).size
    val potentiallyChangedFiles = f2Map.keySet.intersect(f1Map.keySet)
    val numPotentiallyChangedFiles = potentiallyChangedFiles.size
    println("numNewFiles: " + numNewFiles)
    println("numDeletedFiles: " + numDeletedFiles)
    println("numChangedFiles (potentially) : " + numPotentiallyChangedFiles)
    println("Size Last Day (raw json): " + (f1Map.values.sum / 1000000000.0)+ "GB")
    println("Size This Day (raw json): " + (f2Map.values.sum / 1000000000.0)+ "GB")
    val filesWithChanges = potentiallyChangedFiles.map(f => {
          val version1 = f1Map(f)
          val version2 = f2Map(f)
          if(version1 != version2)
            Option((f,(version1-version2).abs))
          else
            None
        })
      .filter(_.isDefined)
      .map(_.get)
    println("Actually Changed Files: " + filesWithChanges.size)
    println("Top five changes: ")
    filesWithChanges.toIndexedSeq.sortBy(t => -t._2)
      .take(5)
      .foreach(println(_))
  }

  zipVariant()

}
