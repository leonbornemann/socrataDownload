package de.hpi.dataset_versioning.data.metadata.custom

import java.time.LocalDate

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.matching.DatasetInstance

case class CustomMetadataCollection(metadata:Map[DatasetInstance,CustomMetadata]) extends JsonWritable[CustomMetadataCollection]{

  val metadataByIntID = metadata.map{case(_,c) => (c.intID,c)}
}

object CustomMetadataCollection extends JsonReadable[CustomMetadataCollection]
