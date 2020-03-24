package de.hpi.dataset_versioning.data.metadata.custom.joinability

import java.io.File

import scala.collection.mutable
import scala.io.Source

class JoinabilityGraph() {

  def initAdjacencyListGroupedByDSAndCol() = {
    for (ds1 <- adjacencyListGroupedByDS.keySet) {
      for ((ds2, edges) <- adjacencyListGroupedByDS(ds1)) {
        edges.foreach{ case ((c1,c2),f) => {
          val map = adjacencyListGroupedByDSAndCol.getOrElseUpdate((ds1,c1),mutable.HashMap[(Int,Short),Float]())
          map.put((ds2,c2),f)
        }}
      }
    }
  }

  private def setEdgeValue(scrDSID: Int, targetDSID: Int, scrColID: Short, targetColID: Short, threshold: Float) = {
    //one-way
    val map = adjacencyListGroupedByDS.getOrElseUpdate(scrDSID,mutable.HashMap())
    .getOrElseUpdate(targetDSID,mutable.HashMap())
    map((scrColID,targetColID)) = threshold
    //second-way
  }

  //to speed up getOrElse calls (avoids object creation)
  private val emptyMap = Map[Int,Map[(Short,Short),(Float,Float)]]()
  private val emptyMap2 = Map[(Short,Short),Float]()

  def hasEdge(scrDSID: Int, targetDSID: Int, scrColID: Short, targetColID: Short): Boolean = {
    adjacencyListGroupedByDS.getOrElse(scrDSID,emptyMap).getOrElse(targetDSID,emptyMap2).contains((scrColID,targetColID))
  }

  def getEdgeValue(scrDSID: Int, targetDSID: Int, scrColID: Short, targetColID: Short) = adjacencyListGroupedByDS(scrDSID)(targetDSID)((scrColID,targetColID))

  val adjacencyListGroupedByDS = mutable.HashMap[Int,mutable.Map[Int,mutable.Map[(Short,Short),Float]]]()
  val adjacencyListGroupedByDSAndCol = mutable.HashMap[(Int,Short),mutable.Map[(Int,Short),Float]]()

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
      val (scrDSID,scrColID,targetDSID,targetColID,threshold) = (tokens(0).toInt,tokens(1).toShort,tokens(2).toInt,tokens(3).toShort,tokens(4).toFloat)
      assert(!graph.hasEdge(scrDSID,targetDSID,scrColID,targetColID))
      graph.setEdgeValue(scrDSID,targetDSID,scrColID,targetColID,threshold)
    }
    graph.initAdjacencyListGroupedByDSAndCol()
    graph
  }
}
