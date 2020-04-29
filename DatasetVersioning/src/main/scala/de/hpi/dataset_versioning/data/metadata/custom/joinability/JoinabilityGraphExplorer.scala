package de.hpi.dataset_versioning.data.metadata.custom.joinability

import java.io.{File, PrintWriter}
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.matching.DatasetInstance

import scala.io.Source

class JoinabilityGraphExplorer() extends StrictLogging {

  def transformToSmallRepresentation(startVersion:LocalDate,endVersion:LocalDate,joinablilityGraphFileOld:File) = {
    val lineIterator = Source.fromFile(joinablilityGraphFileOld).getLines()
    IOService.cacheCustomMetadata(startVersion,endVersion)
    val mdCollection = IOService.cachedCustomMetadata((startVersion,endVersion)).metadata
    val thresholds = Seq(0.8f,0.9f,1.0f)
    lineIterator.next()
    val joinablilityGraphFileNew = IOService.getJoinabilityGraphFile(startVersion,endVersion)
    val pr = new PrintWriter(joinablilityGraphFileNew)
    var count = 0
    var errCount = 0
    var dsErrCount = 0
    pr.println("scrDSID,srcDSDate,scrColID,targetDSID,targetDSDate,targetColID,containmentOfSrcInTarget,maxUniqueness")
    while(lineIterator.hasNext){
      val tokens = lineIterator.next().split(",")
      assert(tokens.size==9)
      val (sourceTable,sourceDate,sourceAttr,targetTable,targetDate,targetAttr) = (tokens(0),LocalDate.parse(tokens(1),IOService.dateTimeFormatter),tokens(2),tokens(3),LocalDate.parse(tokens(4),IOService.dateTimeFormatter),tokens(5))
      val existsAtThreshold = tokens.slice(6,tokens.size).map(_.toBoolean)
      val index = existsAtThreshold.lastIndexOf(true)
      if(index != -1) {
        val highestThreshold = thresholds(index)
        if(mdCollection.contains(DatasetInstance(sourceTable,sourceDate)) && mdCollection.contains(DatasetInstance(targetTable,targetDate))) {
          val srcMetadata = mdCollection(DatasetInstance(sourceTable,sourceDate))
          val targetMetadata = mdCollection(DatasetInstance(targetTable,targetDate))
          val (srcID, targetID) = (srcMetadata.intID, targetMetadata.intID)
          if (srcMetadata.columnMetadata.contains(sourceAttr) && targetMetadata.columnMetadata.contains(targetAttr)) {
            val srcColMetadata = srcMetadata.columnMetadata(sourceAttr)
            val targetColMetadata = targetMetadata.columnMetadata(targetAttr)
            val srcColID = srcColMetadata.shortID
            val targetColID = targetColMetadata.shortID
            val maxUniqueness = Math.max(srcColMetadata.uniqueness, targetColMetadata.uniqueness)
            pr.println(s"$srcID,${sourceDate.format(IOService.dateTimeFormatter)},$srcColID,$targetID,${targetDate.format(IOService.dateTimeFormatter)},$targetColID,$highestThreshold,$maxUniqueness")
          } else {
            errCount += 1
          }
        } else{
          dsErrCount+=1
        }
      }
      if(count%100000==0) logger.debug(s"$count,$dsErrCount,$errCount")
      count +=1
    }
    pr.close()
  }

  def exploreGraph(graph: JoinabilityGraph) = {
    logger.debug(s"numEdges: ${graph.numEdges()}")
    System.gc()
    logger.debug("Graph loaded - check htop now, afterwards press enter")
    var a = scala.io.StdIn.readLine()
    logger.debug("continuing")
    graph.switchToAdjacencyListGroupedByDSAndCol()
    logger.debug(s"numEdges: ${graph.numEdges()}")
    System.gc()
    logger.debug("Representation switched - check htop now, afterwards press enter")
    a = scala.io.StdIn.readLine()
    logger.debug("continuing")
  }

  def exploreGraphMemory(startVersion:LocalDate,endVersion:LocalDate) = {
    logger.debug("Nothing loaded - press enter to start")
    val a = scala.io.StdIn.readLine()
    logger.debug("loading filtered graph")
    var graph = JoinabilityGraph.readGraphFromGoOutput(IOService.getJoinabilityGraphFile(startVersion,endVersion),1.0f)
    exploreGraph(graph)
    graph = null
    System.gc()
    logger.debug("loading unfiltered graph")
    graph = JoinabilityGraph.readGraphFromGoOutput(IOService.getJoinabilityGraphFile(startVersion,endVersion))
    exploreGraph(graph)

  }

  def explore(path: String) = {
    val lineIterator = Source.fromFile(path).getLines()
    val thresholds = Seq(0.8f,0.9f,1.0f)
    val edges = scala.collection.mutable.HashMap[(Int,Short,Int,Short),Float]()
    var count = 0
    val uniquenessThreshold = 0.95
    lineIterator.next()
    while(lineIterator.hasNext){
      val tokens = lineIterator.next().split(",")
      assert(tokens.size==6)
      //pr.println("scrDSID,scrColID,targetDSID,targetColID,highestThreshold,highestUniqueness")
      val (scrDSID,scrColID,targetDSID,targetColID,highestThreshold,highestUniqueness) = (tokens(0).toInt,tokens(1).toShort,tokens(2).toInt,tokens(3).toShort,tokens(4).toFloat,tokens(5).toFloat)
      val edgeId = (scrDSID,scrColID,targetDSID,targetColID)
      if(highestUniqueness > uniquenessThreshold) {
        val storedContainmentThreshold = edges.getOrElse(edgeId, -1.0f)
        if (highestThreshold > storedContainmentThreshold) {
          edges(edgeId) = highestThreshold
        }
      }
      if(count%1000000==0) logger.debug(s"$count")
      count +=1
    }
    var t1,t2,t3 = 0
    val it = edges.iterator
    count = 0
    while(it.hasNext){
      val e = it.next()
      if(e._2>= thresholds(0)) t1 +=1
      if(e._2>= thresholds(1)) t2 +=1
      if(e._2>= thresholds(2)) t3 +=1
      if(count%1000000==0) logger.debug(s"${count / edges.size.toDouble}%")
      count +=1
    }
    logger.debug(s"${thresholds.zip(Seq(t1,t2,t3))}")
  }

}
