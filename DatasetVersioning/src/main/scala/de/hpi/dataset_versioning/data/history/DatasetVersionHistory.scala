package de.hpi.dataset_versioning.data.history

import java.time.LocalDate

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}

import scala.collection.mutable

class DatasetVersionHistory(val id:String,
                            val versionsWithChanges:mutable.ArrayBuffer[LocalDate] = mutable.ArrayBuffer[LocalDate](),
                            val deletions:mutable.ArrayBuffer[LocalDate] = mutable.ArrayBuffer[LocalDate]()) extends JsonWritable[DatasetVersionHistory]{


}

object DatasetVersionHistory extends JsonReadable[DatasetVersionHistory]
