package de.hpi.dataset_versioning.data.metadata.custom

import java.time.LocalDate

import de.hpi.dataset_versioning.data.JsonWritable

case class CustomMetadata(id:String,
                          intID:Int, //unique for all versions of all datasets
                          version:LocalDate,
                          nrows:Int,
                          schemaSpecificHash:Int,
                          tupleSpecificHash:Int,
                          columnMetadata: Map[String,ColumnCustomMetadata], //maps colname to its metadata
                         ) extends JsonWritable[CustomMetadata]{

  def ncols = columnMetadata.size
  val columnMetadataByID = columnMetadata.map{case(_,cm) => (cm.shortID,cm)}

}
