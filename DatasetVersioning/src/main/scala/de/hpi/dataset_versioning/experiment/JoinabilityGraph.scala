package de.hpi.dataset_versioning.experiment

import java.io.File

import scala.collection.mutable
import scala.io.Source

class JoinabilityGraph() {

  def setEdgeValue(scrDSID: Int, targetDSID: Int, scrColID: Short, targetColID: Short, highestThreshold: Float) = {
    //one-way
    val map = adjacencyList.getOrElseUpdate(scrDSID,mutable.HashMap())
    .getOrElseUpdate(targetDSID,mutable.HashMap())
    map((scrColID,targetColID)) = highestThreshold
    //second-way
    val map2 = adjacencyList.getOrElseUpdate(targetDSID,mutable.HashMap())
      .getOrElseUpdate(scrDSID,mutable.HashMap())
    map2((targetColID,scrColID)) = highestThreshold
  }

  //to speed up getOrElse calls (avoids object creation)
  private val emptyMap = Map[Int,Map[(Short,Short),Float]]()
  private val emptyMap2 = Map[(Short,Short),Float]()

  def hasEdge(scrDSID: Int, targetDSID: Int, scrColID: Short, targetColID: Short): Boolean = {
    adjacencyList.getOrElse(scrDSID,emptyMap).getOrElse(targetDSID,emptyMap2).contains((scrColID,targetColID))
  }

  def getEdgeValue(scrDSID: Int, targetDSID: Int, scrColID: Short, targetColID: Short) = adjacencyList(scrDSID)(targetDSID)((scrColID,targetColID))

  val adjacencyList = mutable.HashMap[Int,mutable.Map[Int,mutable.Map[(Short,Short),Float]]]()
}
object JoinabilityGraph {

  def readGraphFromGoOutput(path: File) = {
    val lineIterator = Source.fromFile(path).getLines()
    val graph = new JoinabilityGraph()
    val uniquenessThreshold = 0.95
    lineIterator.next()
    while(lineIterator.hasNext){
      val tokens = lineIterator.next().split(",")
      assert(tokens.size==6)
      //pr.println("scrDSID,scrColID,targetDSID,targetColID,highestThreshold,highestUniqueness")
      val (scrDSID,scrColID,targetDSID,targetColID,highestThreshold,highestUniqueness) = (tokens(0).toInt,tokens(1).toShort,tokens(2).toInt,tokens(3).toShort,tokens(4).toFloat,tokens(5).toFloat)
      if(highestUniqueness > uniquenessThreshold) {
        val storedContainmentThreshold = if(graph.hasEdge(scrDSID,targetDSID,scrColID,targetColID)) graph.getEdgeValue(scrDSID,targetDSID,scrColID,targetColID) else -1.0
        if (highestThreshold > storedContainmentThreshold) {
          graph.setEdgeValue(scrDSID,targetDSID,scrColID,targetColID,highestThreshold)
        }
      }
    }
    graph
  }
}
