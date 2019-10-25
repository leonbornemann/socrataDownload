package de.hpi.dataset_versioning.crawl.data

case class DatasetMetadata(resource:Resource,
                           classification:Classification,
                           metadata:AdditionalMetadata,
                           permalink:String,
                           link:String,
                           preview_image_url:Option[String],
                           owner:User,
                           published_copy:Option[Any])

object DatasetMetadata extends JsonReadable[DatasetMetadata]