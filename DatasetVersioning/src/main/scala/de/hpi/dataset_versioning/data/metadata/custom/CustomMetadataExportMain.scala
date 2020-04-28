package de.hpi.dataset_versioning.data.metadata.custom

import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable.HashMap

object CustomMetadataExportMain extends App with StrictLogging{

  IOService.socrataDir = args(0)
  val version = LocalDate.parse(args(1),IOService.dateTimeFormatter)
  //read

  private def read = {
    val mdColelction = CustomMetadataCollection.fromJsonFile(IOService.getCustomMetadataFile(version).getAbsolutePath)
    val histogram = mdColelction.metadata.values.groupBy(_.tupleSpecificHash)
      .mapValues(_.size)
      .values.toSeq
      .groupBy(identity)
      .mapValues(_.size)
    histogram.foreach(println(_))
  }

  def export() = {
    val files = IOService.extractDataToWorkingDir(version)
    val metadataCollection = HashMap[String,CustomMetadata]()
    var count = 0
    files.foreach(f => {
      val ds = IOService.tryLoadDataset(IOService.filenameToID(f),version)
      if(!ds.isEmpty){
        val md = ds.extractCustomMetadata
        metadataCollection.put(IOService.filenameToID(f),md)
      }
      count +=1
      if(count%1000 == 0)
        logger.debug(s"finished $count")
    })
    CustomMetadataCollection(metadataCollection.toMap).toJsonFile(IOService.getCustomMetadataFile(version))
  }

  export()


}
