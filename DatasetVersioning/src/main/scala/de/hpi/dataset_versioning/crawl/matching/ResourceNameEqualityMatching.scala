package de.hpi.dataset_versioning.crawl.matching

import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable

class ResourceNameEqualityMatching extends MatchingAlgorithm with StrictLogging{

  override def matchDatasets(previous: Set[Dataset], current: Set[Dataset]) = {
    val resourceNameToDataset = mutable.HashMap[String,Dataset]()
    var undefinedMetadataPrev = 0
    var undefinedMetadataCur = 0
    current.foreach(d => {
      d.loadMetadata()
      if(d.datasetMetadata.isDefined)
        resourceNameToDataset.put(d.datasetMetadata.get.resource.name,d)
      else
        undefinedMetadataPrev+=1

    })
    previous.foreach(d => {
      d.loadMetadata()
      if(d.datasetMetadata.isDefined) {
        val resourceName = d.datasetMetadata.get.resource.name
        if (resourceNameToDataset.contains(resourceName)) {
          matching.put(d, resourceNameToDataset(resourceName))
        }
      }
      else
        undefinedMetadataCur+=1
    })
    logger.trace(s"#no metadata in (previous,current) : ${(undefinedMetadataPrev,undefinedMetadataCur)}")
    matching
  }
}
