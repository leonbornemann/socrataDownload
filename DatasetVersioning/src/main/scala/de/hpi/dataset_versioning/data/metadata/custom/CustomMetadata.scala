package de.hpi.dataset_versioning.data.metadata.custom

import de.hpi.dataset_versioning.data.JsonWritable

case class CustomMetadata(id:String,
                          version:String,
                          nrows:Int,
                          schemaSpecificHash:Int,
                          tupleSpecificHash:Int,
                          columnMetadata: ColumnCustomMetadata,
                         ) extends JsonWritable[CustomMetadata]{

  def ncols = columnMetadata.colHashes.size

}
