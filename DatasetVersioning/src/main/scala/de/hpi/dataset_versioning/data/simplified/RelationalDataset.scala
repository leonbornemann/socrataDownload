package de.hpi.dataset_versioning.data.simplified

import java.time.LocalDate

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}

case class RelationalDataset(id:String,
                             version:LocalDate,
                             attributeNames:IndexedSeq[String],
                             rows:IndexedSeq[RelationalDatasetRow]) extends JsonWritable[RelationalDataset]

object RelationalDataset extends JsonReadable[RelationalDataset]