package de.hpi.dataset_versioning.crawl

object CrawlSummary extends App {

  val summarizer = new CrawlSummarizer(args(0))
  summarizer.allTimeChangeSummary()
}
