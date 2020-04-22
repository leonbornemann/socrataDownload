package de.hpi.dataset_versioning.data.metadata.custom

import de.hpi.dataset_versioning.data.JsonWritable

case class CustomMetadata(id:String,
                          version:String,
                          nrows:Int,
                          schemaSpecificHash:Int,
                          tupleSpecificHash:Int,
                          columnMetadata: Map[String,ColumnCustomMetadata], //maps colname to its metadata
                         ) extends JsonWritable[CustomMetadata]{

  def ncols = columnMetadata.size

}
