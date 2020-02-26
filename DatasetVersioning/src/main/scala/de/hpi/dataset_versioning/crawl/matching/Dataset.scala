package de.hpi.dataset_versioning.crawl.matching

import java.io.File
import java.time.LocalDate

import de.hpi.dataset_versioning.crawl.data.metadata.DatasetMetadata

class Dataset(val id:String, val date:LocalDate) {

  var datasetMetadata:Option[DatasetMetadata] = None

  def loadMetadata(): Unit = {
    if(!datasetMetadata.isDefined)
      datasetMetadata = IOService.getMetadataForDataset(date,id)
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Dataset]

  override def equals(other: Any): Boolean = other match {
    case that: Dataset =>
      (that canEqual this) &&
        id == that.id &&
        date == that.date
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(id, date)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
