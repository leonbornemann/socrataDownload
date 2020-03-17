package de.hpi.dataset_versioning.data

import java.io.{File, PrintWriter}
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.io.IOService

import scala.io.Source

object IdentifierMapping extends StrictLogging{

  def createColumnIDMapping(version: LocalDate) = {
    val files = IOService.extractDataToWorkingDir(version)
    val fileToID = IdentifierMapping.readDatasetIDMapping_stringToInt(IOService.getDatasetIdMappingFile(version))
    logger.debug("Mapping Columns Names to column IDS and extracting uniqueness")
    val columnContentUniqunessFile = new PrintWriter(IOService.getColumnIdMappingFile(version))
    columnContentUniqunessFile.println("dsIDstr,dsIdInt,colname,colID,uniqueness")
    var count = 0
    files.foreach(f => {
      val ds = IOService.tryLoadDataset(f,version)
      if(!ds.erroneous){
        ds.colNames.zipWithIndex.foreach{case (colname,colID) => {
          val col = ds.getColumnObject(colID)
          columnContentUniqunessFile.println(s"${IOService.filenameToID(f)},${fileToID(IOService.filenameToID(f))},$colname,$colID,${col.uniqueness}")
        }}
      }
      count +=1
      if(count %1000 ==0){
        logger.debug(s"Finished $count out of ${files.size} (${100*count/files.size.toDouble}%)")
      }
    })
    columnContentUniqunessFile.close()
  }

  def createDatasetIDMapping(version: LocalDate) = {
    val files = IOService.extractDataToWorkingDir(version)
    val idMappingFile = IOService.getDatasetIdMappingFile(version)
    //write filename to id mapping
    logger.debug("Mapping Dataset Names to IDS")
    val dsIDMapFile = new PrintWriter(idMappingFile)
    val fileToID = files.toIndexedSeq.sortBy(IOService.filenameToID(_))
      .zipWithIndex
      .toMap
    dsIDMapFile.println("idStr,idInt")
    fileToID.foreach{ case(f,i) => {
      dsIDMapFile.println(s"${IOService.filenameToID(f)},$i")
    }}
    dsIDMapFile.close()
  }


  def readDatasetIDMapping_stringToInt(datasetIDMappingFile: File) = {
    Source.fromFile(datasetIDMappingFile).getLines()
      .toSeq
      .zipWithIndex
      .filter(_._2!=0)
      .map(l => {
        val tokens = l._1.split(",")
        (tokens(0),tokens(1).toInt)
      })
      .toMap
  }

  def readDatasetIDMapping_intToString(datasetIDMappingFile: File) = {
    Source.fromFile(datasetIDMappingFile).getLines()
      .toSeq
      .zipWithIndex
      .filter(_._2!=0)
      .map(l => {
        val tokens = l._1.split(",")
        (tokens(1).toInt,tokens(0))
      })
      .toMap
  }

  def readColumnIDMapping_shortToString(columnIDMappingFile: File) = {
    Source.fromFile(columnIDMappingFile).getLines()
      .toSeq
      .zipWithIndex
      .filter(_._2!=0)
      .map(l => {
        val tokens = l._1.split(",")
        ((tokens(1).toInt,tokens(3).toShort),tokens(2))
      })
      .toMap
  }

  def readColumnIDMapping_stringToShort(columnIDMappingFile: File) = {
    Source.fromFile(columnIDMappingFile).getLines()
      .toSeq
      .zipWithIndex
      .filter(_._2!=0)
      .map(l => {
        val tokens = l._1.split(",")
        ((tokens(1).toInt,tokens(2)),(tokens(3).toShort,tokens(4).toDouble))
      })
      .toMap
  }

}
