package de.hpi.dataset_versioning.data.metadata.custom.joinability

import java.io.{File, PrintWriter}
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.metadata.custom.IdentifierMapping
import de.hpi.dataset_versioning.io.IOService

import scala.io.Source

class JoinabilityGraphExplorer() extends StrictLogging {

  def createManualLookupTests(version:LocalDate,joinablilityGraphFileExcerpt:File, dsIDMapFile:File, colIDMapFile:File,outputDir:File) = {
    //scrDSID,scrColID,targetDSID,targetColID,highestThreshold,highestUniqueness
    val edges = Source.fromFile(joinablilityGraphFileExcerpt).getLines().toSeq
      .map(JoinabilityGraphEdge.create(_))
    val dsNameToID = IdentifierMapping.readDatasetIDMapping_intToString(dsIDMapFile)
    val dsIDColNameToColID = IdentifierMapping.readColumnIDMapping_shortToString(colIDMapFile)
    edges.foreach( e => {
      val ds1Name = dsNameToID(e.scrDSID)
      val ds2Name = dsNameToID(e.targetDSID)
      val ds1ColName = dsIDColNameToColID((e.scrDSID,e.scrColID))
      val ds2ColName = dsIDColNameToColID((e.targetDSID,e.targetColID))
      val newDir = new File(outputDir.getAbsolutePath + s"/$ds1Name.${ds1ColName}_to_$ds2Name.$ds2ColName")
      newDir.mkdir()
      val ds1 = IOService.tryLoadDataset(ds1Name,version)
      val ds2 = IOService.tryLoadDataset(ds2Name,version)
      ds1.exportToCSV(new File(newDir.getAbsolutePath + s"/$ds1Name.${ds1ColName}" + ".csv"))
      ds2.exportToCSV(new File(newDir.getAbsolutePath + s"/$ds2Name.${ds2ColName}" + ".csv"))
    })
  }

  def transformToSmallRepresentation(joinablilityGraphFileOld:File, dsIDMapFile:File, colIDMapFile:File,joinablilityGraphFileNew:File) = {
    val lineIterator = Source.fromFile(joinablilityGraphFileOld).getLines()
    val dsNameToID = IdentifierMapping.readDatasetIDMapping_stringToInt(dsIDMapFile)
    val dsIDColNameToColID = IdentifierMapping.readColumnIDMapping_stringToShort(colIDMapFile)
    val thresholds = Seq(0.8f,0.9f,1.0f)
    lineIterator.next()
    val pr = new PrintWriter(joinablilityGraphFileNew)
    var count = 0
    var errCount = 0
    var dsErrCount = 0
    pr.println("scrDSID,scrColID,targetDSID,targetColID,containmentOfSrcInTarget,maxUniqueness")
    while(lineIterator.hasNext){
      val tokens = lineIterator.next().split(",")
      assert(tokens.size==9)
      val (sourceTable,sourceAttr,targetTable,targetAttr) = (tokens(0),tokens(2),tokens(3),tokens(5))
      val existsAtThreshold = tokens.slice(6,tokens.size).map(_.toBoolean)
      val index = existsAtThreshold.lastIndexOf(true)
      if(index != -1) {
        val highestThreshold = thresholds(index)
        if(dsNameToID.contains(sourceTable) && dsNameToID.contains(targetTable)) {
          val (srcID, targetID) = (dsNameToID(sourceTable), dsNameToID(targetTable))
          if (dsIDColNameToColID.contains((srcID, sourceAttr)) && dsIDColNameToColID.contains(targetID, targetAttr)) {
            val (srcColID, srcUniqueness) = dsIDColNameToColID((srcID, sourceAttr))
            val (targetColID, targetUniqueness) = dsIDColNameToColID((targetID, targetAttr))
            val maxUniqueness = Math.max(srcUniqueness, targetUniqueness)
            pr.println(s"$srcID,$srcColID,$targetID,$targetColID,$highestThreshold,$maxUniqueness")
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

  def exploreGraphMemory(version:LocalDate) = {
    logger.debug("Nothing loaded - press enter to start")
    val a = scala.io.StdIn.readLine()
    logger.debug("loading filtered graph")
    var graph = JoinabilityGraph.readGraphFromGoOutput(IOService.getJoinabilityGraphFile(version),1.0f)
    exploreGraph(graph)
    graph = null
    System.gc()
    logger.debug("loading unfiltered graph")
    graph = JoinabilityGraph.readGraphFromGoOutput(IOService.getJoinabilityGraphFile(version))
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
