package de.hpi.dataset_versioning.io

import java.io.{File, FileInputStream, FileOutputStream, StringReader}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.zip.{ZipEntry, ZipInputStream}

import com.google.gson.JsonParser
import com.google.gson.stream.JsonReader
import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.metadata.DatasetMetadata
import de.hpi.dataset_versioning.data.parser.JsonDataParser
import de.hpi.dataset_versioning.matching.DatasetInstance

import scala.collection.mutable
import scala.io.Source

object IOService extends StrictLogging{

  def printSummary() = {
    logger.debug("Running socrata IO Service with the following configuration")
    logger.debug(s"Socrata Main directory: $socrataDir")
    logger.debug(s"Socrata Data directory: $DATA_DIR")
    logger.debug(s"Socrata Metadata directory: $METADATA_DIR")
    logger.debug(s"Socrata Diff directory: $DIFF_DIR")
    logger.debug(s"Compressed Snapshots (Checkpoints) available for: {}",getSortedZippedDatalakeSnapshots)
    logger.debug(s"Compressed Diffs available for: {}",getSortedZippedDiffs)
    logger.debug(s"extracted (uncompressed) snapshots available for {}",getSortedUncompressedSnapshots)
    logger.debug(s"extracted (uncompressed) diffs available for {}",getSortedUncompressedDiffs)
  }


  def versionExists(date: LocalDate) = getSortedDatalakeVersions.contains(date)

  def diffExists(version: LocalDate) = {
    val diffFile = getZippedDiffFileForDate(version)
    diffFile.exists()
  }

  def jsonFilenameFromID(id: String): String = id + ".json?"
  private val jsonParser = new JsonDataParser


  def loadDataset(prev: DatasetInstance) = {
    val subDirectory = new File(DATA_DIR_UNCOMPRESSED + prev.date.format(dateTimeFormatter) + "/")
    if(!subDirectory.exists())
      throw new AssertionError(s"${subDirectory} must be extracted to working directory first")
    val datasetFile = new File(subDirectory.getAbsolutePath + "/" + jsonFilenameFromID(prev.id))
    if(!datasetFile.exists())
      throw new AssertionError(s"${prev.id} does not exist in ${prev.date}")
    val ds = jsonParser.parseJsonFile(datasetFile)
    ds
  }

  def fileNameToDate(f: File) = LocalDate.parse(filenameWithoutFiletype(f),dateTimeFormatter)

  def filenameToID(f: File): String = filenameWithoutFiletype(f)

  def DATA_DIR = socrataDir + "/data/"
  def METADATA_DIR = socrataDir + "/metadata/"
  def DIFF_DIR = socrataDir + "/diff/"
  def WORKING_DIR:String = socrataDir + "/workingDir/"
  def DATA_DIR_UNCOMPRESSED = WORKING_DIR + "/snapshots/"
  def DIFF_DIR_UNCOMPRESSED = WORKING_DIR + "/diffs/"

  private def filenameWithoutFiletype(f: File) = {
    f.getName.split("\\.")(0)
  }

  def getZippedDataFileForDate(date: LocalDate): File = new File(DATA_DIR + date.format(dateTimeFormatter) + ".zip")
  def getZippedDiffFileForDate(date: LocalDate): File = new File(DIFF_DIR + date.format(dateTimeFormatter) + "_diff.zip")
  def getDiffDirForDate(date: LocalDate): File = new File(DIFF_DIR + date.format(dateTimeFormatter) + "_diff")
  def getUncompressedDiffDirForDate(date: LocalDate) = new File(DIFF_DIR_UNCOMPRESSED + date.format(dateTimeFormatter) + "_diff")

  def extractDataToWorkingDir(date: LocalDate): Set[File] = {
    val zipFile: File = getZippedDataFileForDate(date)
    val subDirectory = new File(DATA_DIR_UNCOMPRESSED + filenameWithoutFiletype(zipFile))
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

  def getSortedZippedDatalakeSnapshots = zippedFilesToSortedDates(DATA_DIR)

  def getSortedUncompressedDiffs = new File(DIFF_DIR_UNCOMPRESSED).listFiles()
    .map(f => LocalDate.parse(f.getName.split("_")(0), dateTimeFormatter))
    .sortBy(_.toEpochDay)

  def getSortedUncompressedSnapshots = new File(DATA_DIR_UNCOMPRESSED).listFiles()
    .map(f => LocalDate.parse(filenameWithoutFiletype(f), dateTimeFormatter))
    .sortBy(_.toEpochDay)

  def zippedFilesToSortedDates(dir: String) = new File(dir)
    .listFiles()
    .filter(_.getName.endsWith(".zip"))
    .map(f => LocalDate.parse(filenameWithoutFiletype(f), dateTimeFormatter))
    .sortBy(_.toEpochDay)

  def getSortedZippedDiffs = new File(DIFF_DIR)
    .listFiles()
    .filter(_.getName.endsWith(".zip"))
    .map(f => LocalDate.parse(f.getName.split("_")(0), dateTimeFormatter))
    .sortBy(_.toEpochDay)

  def getSortedDatalakeVersions() = {
    val dates = (getSortedZippedDatalakeSnapshots ++ getSortedZippedDiffs)
    dates.toSet
      .toIndexedSeq
      .sortBy((t:LocalDate) => t.toEpochDay)
  }
}
