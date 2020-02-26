package de.hpi.dataset_versioning.crawl

import java.io.{File, FileInputStream, PrintWriter, StringWriter}
import java.util.zip.{ZipEntry, ZipInputStream}

import com.google.gson.{JsonElement, JsonObject, JsonPrimitive}

import scala.collection.mutable

object RecentCrawlSummaryMain extends App{
  val summarizer = new CrawlSummarizer(args(0),Some(args(1)))
  summarizer.recentCrawlSummary()

}
