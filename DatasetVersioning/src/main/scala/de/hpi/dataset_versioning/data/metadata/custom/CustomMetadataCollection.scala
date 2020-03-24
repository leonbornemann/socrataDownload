package de.hpi.dataset_versioning.data.metadata.custom

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}

case class CustomMetadataCollection(metadata:Map[String,CustomMetadata]) extends JsonWritable[CustomMetadataCollection]

object CustomMetadataCollection extends JsonReadable[CustomMetadataCollection]
