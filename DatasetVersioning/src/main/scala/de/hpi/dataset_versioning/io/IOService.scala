package de.hpi.dataset_versioning.io

import java.io.{File, FileInputStream, FileOutputStream, StringReader}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.zip.{ZipEntry, ZipInputStream}

import com.google.gson.JsonParser
import com.google.gson.stream.JsonReader
import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.LoadedRelationalDataset
import de.hpi.dataset_versioning.data.metadata.DatasetMetadata
import de.hpi.dataset_versioning.data.metadata.custom.{CustomMetadata, CustomMetadataCollection}
import de.hpi.dataset_versioning.data.parser.JsonDataParser
import de.hpi.dataset_versioning.matching.DatasetInstance

import scala.sys.process._
import scala.collection.mutable
import scala.io.Source
import scala.reflect.io.Directory

object IOService extends StrictLogging{

  def saveDeleteCompressedDataFile(version: LocalDate): Unit = {
    //check if we can safely delete this
    val checkpointsBefore = getCheckpoints()
      .filter(_.isBefore(version))
    if(checkpointsBefore.isEmpty){
      logger.warn(s"No checkpoint found before $version - will not delete!")
    } else{
      val latestCheckpoint = checkpointsBefore.last
      var curDiffVersion = latestCheckpoint.plusDays(1)
      var deleteIsSafe = true
      while(curDiffVersion.toEpochDay <=version.toEpochDay && deleteIsSafe){
        deleteIsSafe = deleteIsSafe && diffExists(curDiffVersion)
        curDiffVersion = curDiffVersion.plusDays(1)
      }
      if(deleteIsSafe) {
        logger.debug(s"Safely deleting $version")
        getCompressedDataFile(version).delete()
      } else{
        logger.debug(s"Can not safely delete $version")
      }
    }
  }


  var socrataDir:String = null
  val dateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE
  val cachedMetadata = mutable.Map[LocalDate,mutable.Map[String,DatasetMetadata]]() //TODO: shrink this cache at some point - use caching library?: https://stackoverflow.com/questions/3651313/how-to-cache-results-in-scala
  val cachedCustomMetadata = mutable.Map[LocalDate,Map[String,CustomMetadata]]() //TODO: shrink this cache at some point - use caching library?: https://stackoverflow.com/questions/3651313/how-to-cache-results-in-scala
  val datasetCache = mutable.Map[DatasetInstance,LoadedRelationalDataset]()

  def cacheCustomMetadata(version: LocalDate) = {
    val mdColelction = CustomMetadataCollection.fromJsonFile(IOService.getCustomMetadataFile(version).getAbsolutePath)
    cachedCustomMetadata(version) = mdColelction.metadata
  }

  def clearUncompressedSnapshot(date: LocalDate) = new Directory(getUncompressedDataDir(date)).deleteRecursively() //TODO: existance checks?
  def clearUncompressedDiff(date: LocalDate) = new Directory(getUncompressedDiffDir(date)).deleteRecursively()

  def printSummary() = {
    logger.debug("Running socrata IO Service with the following configuration")
    logger.debug(s"Socrata Main directory: $socrataDir")
    logger.debug(s"Socrata Data directory: $DATA_DIR")
    logger.debug(s"Socrata Metadata directory: $METADATA_DIR")
    logger.debug(s"Socrata Diff directory: $DIFF_DIR")
    logger.debug(s"Compressed Snapshots (Checkpoints) available for: {}",getSortedZippedDatalakeSnapshots)
    logger.debug(s"extracted (uncompressed) snapshots available for {}",getSortedUncompressedSnapshots)
    logger.debug(s"Compressed Diffs available for: {}",getSortedZippedDiffs)
    logger.debug(s"extracted (uncompressed) diffs available for {}",getSortedUncompressedDiffs)
  }

  def compressedSnapshotExists(date: LocalDate) = getCompressedDataFile(date).exists()
  def compressedDiffExists(version: LocalDate) = getCompressedDiffFile(version).exists()
  def uncompressedSnapshotExists(version: LocalDate) = getUncompressedDataDir(version).exists() && !getUncompressedDataDir(version).listFiles().isEmpty
  def uncompressedDiffExists(version: LocalDate) = getUncompressedDiffDir(version).exists() && !getUncompressedDiffDir(version).listFiles().isEmpty
  def snapshotExists(date: LocalDate) = compressedSnapshotExists(date) || uncompressedSnapshotExists(date)
  def diffExists(version: LocalDate): Boolean = uncompressedDiffExists(version) || compressedDiffExists(version)

  def versionExists(date: LocalDate) = getSortedDatalakeVersions.contains(date)


  def jsonFilenameFromID(id: String): String = id + ".json?"
  private val jsonParser = new JsonDataParser

  def loadDataset(datasetInstance: DatasetInstance,skipParseExceptions:Boolean = false) = {
    if(datasetCache.contains(datasetInstance))
      datasetCache(datasetInstance)
    else {
      val subDirectory = new File(DATA_DIR_UNCOMPRESSED + datasetInstance.date.format(dateTimeFormatter) + "/")
      if (!subDirectory.exists())
        throw new AssertionError(s"${subDirectory} must be extracted to working directory first")
      val datasetFile = new File(subDirectory.getAbsolutePath + "/" + jsonFilenameFromID(datasetInstance.id))
      if (!datasetFile.exists())
        throw new AssertionError(s"${datasetInstance.id} does not exist in ${datasetInstance.date}")
      if (skipParseExceptions) {
        val ds = jsonParser.tryParseJsonFile(datasetFile, datasetInstance.id, datasetInstance.date)
        if (ds.isDefined) {
          ds.get
        }
        else {
          val ds = new LoadedRelationalDataset(datasetInstance.id, datasetInstance.date)
          ds.erroneous = true
          ds
        }
      } else {
        jsonParser.parseJsonFile(datasetFile, datasetInstance.id, datasetInstance.date)
      }
    }
  }

  def tryLoadDataset(datasetInstance:DatasetInstance) = {
    loadDataset(datasetInstance,true)
  }

  def tryLoadDataset(file:File,version:LocalDate) = {
    loadDataset(new DatasetInstance(filenameToID(file),version),true)
  }

  def tryLoadDataset(id:String,version:LocalDate) = {
    loadDataset(new DatasetInstance(id,version),true)
  }

  def tryLoadAndCacheDataset(id:String,version:LocalDate) = {
    val ds = loadDataset(new DatasetInstance(id,version),true)
    cacheDataset(ds)
    ds
  }

  def cacheDataset(ds:LoadedRelationalDataset) = {
    datasetCache.put(new DatasetInstance(ds.id,ds.version),ds)
    if(datasetCache.size%100 == 0){
      logger.trace(s"Current Dataset Cache Size: ${datasetCache.size}")
    }
  }

  def fileNameToDate(f: File) = LocalDate.parse(filenameWithoutFiletype(f),dateTimeFormatter)

  def filenameToID(f: File): String = filenameWithoutFiletype(f)

  def DATA_DIR = socrataDir + "/data/"
  def METADATA_DIR = socrataDir + "/metadata/"
  def SNAPSHOT_METADATA_DIR = socrataDir + "/snapshotMetadata/"
  def DIFF_DIR = socrataDir + "/diff/"
  def WORKING_DIR:String = socrataDir + "/workingDir/"
  def VERSION_HISTORY_METADATA_DIR = socrataDir + "/versionHistory/"
  def DATA_DIR_UNCOMPRESSED = WORKING_DIR + "/snapshots/"
  def DIFF_DIR_UNCOMPRESSED = WORKING_DIR + "/diffs/"

  private def filenameWithoutFiletype(f: File) = {
    f.getName.split("\\.")(0)
  }

  //snapshotMetadataFiles:
  def getJoinabilityGraphFile(date:LocalDate) = new File(SNAPSHOT_METADATA_DIR + date.format(dateTimeFormatter) + "/smallJoinabilityGraph.csv")
  def getDatasetIdMappingFile(date: LocalDate) = new File(SNAPSHOT_METADATA_DIR + date.format(dateTimeFormatter) + "/dsIDMap.csv")
  def getColumnIdMappingFile(date:LocalDate) = new File(SNAPSHOT_METADATA_DIR + date.format(dateTimeFormatter) + "/columnIDMap.csv")
  def getInferredProjectionFile(date: LocalDate) = new File(SNAPSHOT_METADATA_DIR + date.format(dateTimeFormatter) + "/inferredProjections.csv")
  def getInferredJoinFile(date: LocalDate) = new File(SNAPSHOT_METADATA_DIR + date.format(dateTimeFormatter) + "/inferredJoins.csv")
  def getCustomMetadataFile(date: LocalDate) = new File(SNAPSHOT_METADATA_DIR + date.format(dateTimeFormatter) + "/customMetadata.json")
  def getVersionHistoryFile() = new File(VERSION_HISTORY_METADATA_DIR + "/datasetVersionHistory.csv")
  //data and diff files
  def getCompressedDataFile(date: LocalDate): File = new File(DATA_DIR + date.format(dateTimeFormatter) + ".zip")
  def getCompressedDiffFile(date: LocalDate): File = new File(DIFF_DIR + date.format(dateTimeFormatter) + "_diff.zip")


  def createAndReturn(file: File) = {
    if(!file.exists()) file.mkdirs()
    file
  }

  //def getDiffDirForDate(date: LocalDate): File = new File(DIFF_DIR + date.format(dateTimeFormatter) + "_diff")
  def getUncompressedDiffDir(date: LocalDate) = createAndReturn(new File(DIFF_DIR_UNCOMPRESSED + date.format(dateTimeFormatter) + "_diff"))
  def getUncompressedDataDir(date: LocalDate) = createAndReturn(new File(DATA_DIR_UNCOMPRESSED + date.format(dateTimeFormatter)))
  def getSnapshotMetadataDir(date: LocalDate) = createAndReturn(new File(SNAPSHOT_METADATA_DIR + date.format(dateTimeFormatter)))

  private def compressToFile(sourceDir: File,targetDir:File) = {
    logger.debug(s"Compressing data from ${sourceDir.getAbsolutePath} to ${targetDir.getAbsolutePath}")
    val toExecute = s"zip -q -r ${targetDir.getAbsolutePath + "/" + sourceDir.getName}.zip $sourceDir"
    toExecute!;
  }

  def compressDataFromWorkingDir(version: LocalDate) = {
    if(!compressedSnapshotExists(version))
      compressToFile(getUncompressedDataDir(version),new File(DATA_DIR))
    else{
      logger.debug(s"skipping data compression of $version, because it already exists")
    }
  }

  def compressDiffFromWorkingDir(version: LocalDate) = {
    if(!compressedDiffExists(version))
      compressToFile(getUncompressedDiffDir(version),new File(DIFF_DIR))
    else{
      logger.debug(s"skipping diff compression of $version, because it already exists")
    }
  }

  def extractDiffToWorkingDir(date: LocalDate) = {
    val zippedDiffFile: File = getCompressedDiffFile(date)
    val subDirectory = new File(DIFF_DIR_UNCOMPRESSED + filenameWithoutFiletype(zippedDiffFile))
    extractZipFile(zippedDiffFile,subDirectory)
  }

  def extractDataToWorkingDir(date: LocalDate): Set[File] = {
    val zipFile: File = getCompressedDataFile(date)
    val subDirectory = new File(DATA_DIR_UNCOMPRESSED + filenameWithoutFiletype(zipFile))
    extractZipFile(zipFile, subDirectory)
  }

  private def extractZipFile(zipFile: File, subDirectory: File) = {
    if (subDirectory.exists()) {
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
    .filter(!_.listFiles.isEmpty)
    .map(f => LocalDate.parse(f.getName.split("_")(0), dateTimeFormatter))
    .sortBy(_.toEpochDay)

  def getSortedUncompressedSnapshots = new File(DATA_DIR_UNCOMPRESSED).listFiles()
    .filter(!_.listFiles.isEmpty)
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

  def getCheckpoints() = (getSortedUncompressedSnapshots ++ getSortedZippedDatalakeSnapshots)
    .toSet
    .toIndexedSeq
    .sortBy((t:LocalDate) => t.toEpochDay)
}
