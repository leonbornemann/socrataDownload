package de.hpi.dataset_versioning.data.history

import java.time.LocalDate

import scala.collection.mutable

class DatasetVersionHistory(id:String,
                            val versionsWithChanges:mutable.ArrayBuffer[LocalDate] = mutable.ArrayBuffer[LocalDate](),
                            val deletions:mutable.ArrayBuffer[LocalDate] = mutable.ArrayBuffer[LocalDate]()) {


}
