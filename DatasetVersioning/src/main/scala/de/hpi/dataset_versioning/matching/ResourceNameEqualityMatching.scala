package de.hpi.dataset_versioning.matching

import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable

class ResourceNameEqualityMatching extends MatchingAlgorithm with StrictLogging{

  override def matchDatasets(previous: Set[DatasetInstance], current: Set[DatasetInstance]) = {
    val resourceNameToDataset = mutable.HashMap[String,DatasetInstance]()
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
          matching.matchings.put(d, resourceNameToDataset(resourceName))
        } else{
          matching.deletes.add(d)
        }
      }
      else
        undefinedMetadataCur+=1
    })
    val matchesAndInsertIds = (matching.matchings.keySet ++ matching.deletes).map(_.id)
    current.filter(ds => !matchesAndInsertIds.contains(ds.id)).foreach(matching.inserts.add(_))
    logger.trace(s"#no metadata in (previous,current) : ${(undefinedMetadataPrev,undefinedMetadataCur)}")
    matching
  }
}
