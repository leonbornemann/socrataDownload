package de.hpi.dataset_versioning.crawl.matching

import java.io.{File, FileInputStream, FileOutputStream, StringReader}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.zip.{ZipEntry, ZipInputStream}

import com.google.gson.JsonParser
import com.google.gson.stream.JsonReader
import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.crawl.MetadataAnalysisMain.idToFilename
import de.hpi.dataset_versioning.crawl.data.metadata.DatasetMetadata

import scala.collection.mutable
import scala.io.Source

object IOService extends StrictLogging{
  def fileNameToDate(f: File) = LocalDate.parse(filenameWithoutFiletype(f),dateTimeFormatter)

  def filenameToID(f: File): String = filenameWithoutFiletype(f)

  def DATA_DIR = socrataDir + "/data/"
  def METADATA_DIR = socrataDir + "/metadata/"

  private def filenameWithoutFiletype(f: File) = {
    f.getName.split("\\.")(0)
  }

  def getZippedDataFileForDate(date: LocalDate): File = new File(DATA_DIR + date.format(dateTimeFormatter) + ".zip")

  def extractDataToWorkingDir(date: LocalDate): Set[File] = {
    val zipFile: File = getZippedDataFileForDate(date)
    val subDirectory = new File(workingDir + "/" + filenameWithoutFiletype(zipFile))
    if(subDirectory.exists()){
      logger.warn(s"subdirectory ${subDirectory.getAbsolutePath} already exists, skipping .zip extraction")
      subDirectory.listFiles().toSet
    } else {
      assert(!subDirectory.exists())
      subDirectory.mkdir()
      assert(zipFile.getName.endsWith(".zip"))
      val buffer = new Array[Byte](1024)
      val zis: ZipInputStream = new ZipInputStream(new FileInputStream(zipFile))
      var ze: ZipEntry = zis.getNextEntry();
      val extractedFiles = mutable.HashSet[File]()
      while (ze != null) {
        if (!ze.isDirectory) {
          val nameTokens = ze.getName.split("/")
          val name = nameTokens(nameTokens.size - 1)
          val newFile = new File(subDirectory.getAbsolutePath + File.separator + name);
          extractedFiles += newFile
          val fos = new FileOutputStream(newFile);
          var len: Int = zis.read(buffer);
          while (len > 0) {
            fos.write(buffer, 0, len)
            len = zis.read(buffer)
          }
          fos.close()
        }
        ze = zis.getNextEntry
      }
      zis.close()
      extractedFiles.toSet
    }
  }

  var socrataDir:String = null
  var workingDir:String = null

  val dateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE

  val cachedMetadata = mutable.Map[LocalDate,mutable.Map[String,DatasetMetadata]]() //TODO: shrink this cache at some point - use caching library?: https://stackoverflow.com/questions/3651313/how-to-cache-results-in-scala

  def cacheMetadata(localDate: LocalDate) = {
    cachedMetadata(localDate) = mutable.HashMap()
    val metadataDir = new File(METADATA_DIR + localDate.format(dateTimeFormatter) + "/")
    val metadataFiles = metadataDir.listFiles()
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
        cachedMetadata(localDate).put(datasetMetadata.resource.id,datasetMetadata)
      })
    })
    logger.debug(s"Added $localDate to metadata cache")
  }

  def getMetadataForDataset(localDate: LocalDate, datasetID: String): Option[DatasetMetadata] = {
      if(!cachedMetadata.contains(localDate)){
        cacheMetadata(localDate)
      }
      if(!cachedMetadata(localDate).contains(datasetID)){
        None
      } else {
        Some(cachedMetadata(localDate)(datasetID))
      }
  }

  def getSortedZippedDatalakeFiles() = new File(DATA_DIR)
    .listFiles()
    .filter(_.getName.endsWith(".zip"))
    .sortBy(_.getName)

  def getSortedDatalakeVersions() = new File(DATA_DIR)
    .listFiles()
    .filter(_.getName.endsWith(".zip"))
    .map(f => LocalDate.parse(filenameWithoutFiletype(f),dateTimeFormatter))
    .sorted
}
