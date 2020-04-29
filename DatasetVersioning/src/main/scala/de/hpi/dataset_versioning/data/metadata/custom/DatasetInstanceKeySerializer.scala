package de.hpi.dataset_versioning.data.metadata.custom

import java.time.LocalDate

import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.matching.DatasetInstance
import org.json4s.CustomKeySerializer

case object DatasetInstanceKeySerializer extends CustomKeySerializer[DatasetInstance](format => (
  { case s:String => DatasetInstance(s.split(",")(0),LocalDate.parse(s.split(",")(1),IOService.dateTimeFormatter))},
  { case i:DatasetInstance => s"${i.id},${i.date.format(IOService.dateTimeFormatter)}" }
)){

}
