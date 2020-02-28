package de.hpi.dataset_versioning.data

import java.io.{File, FileInputStream}

import de.hpi.dataset_versioning.data.metadata.Provenance
import org.json4s.jackson.JsonMethods.parse
import org.json4s.{DefaultFormats, _}
import org.json4s.ext.EnumNameSerializer

import scala.io.Source

trait JsonReadable[T<:AnyRef] {

  implicit val formats = DefaultFormats + new EnumNameSerializer(Provenance) //+ new EnumNameSerializer(KeyType)


  def fromJsonString(json: String)(implicit m:Manifest[T]) = {
    parse(json).extract[T]
  }

  def fromJsonFile(path: String)(implicit m:Manifest[T]) = {
    //val string = Source.fromFile(path).getLines().mkString("\n")
    val file = new FileInputStream( new File(path))
    val json = parse(file)
    json.extract[T]
  }

  def fromJsonObjectPerLineFile(path:String)(implicit m:Manifest[T]):Iterator[T] = {
    Source.fromFile(path).getLines()
      .map(fromJsonString(_))
  }
}
