package de.hpi.dataset_versioning.data.metadata.custom.joinability

import java.io.File

import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable
import scala.io.Source

class JoinabilityGraph() extends StrictLogging{
  def numEdges() = {
    if(groupedByDSRepresentation)
      adjacencyListGroupedByDS.mapValues(_.mapValues(_.size).values.sum).values.sum
    else
      adjacencyListGroupedByDSAndCol.values.map(_.size).sum
  }


  val NO_CONTAINMENT_VAL: Float = Float.NegativeInfinity

  private def ensureConsistency(minThreshold1Direction:Float = 1.0f) = {
    val edgesToPrune = mutable.HashSet[(Int,Int,Short,Short)]()
    val toAdd = mutable.HashSet[(Int,Int,Short,Short)]()
    for (ds1 <- adjacencyListGroupedByDS.keySet) {
      for ((ds2, edges) <- adjacencyListGroupedByDS(ds1)) {
        val ds2Connections = adjacencyListGroupedByDS.getOrElse(ds2, mutable.HashMap[Int, mutable.Map[ColEdge, Float]]())
        val ds2Tods1 = ds2Connections.getOrElse(ds1, mutable.Map[ColEdge, Float]())
        edges.foreach { case (ColEdge(c1, c2), f) => {
          val otherEdgeExists = adjacencyListGroupedByDS.contains(ds2) && ds2Connections.contains(ds1) && ds2Tods1.contains(ColEdge(c2,c1))
          val otherVal = if(otherEdgeExists) ds2Tods1(ColEdge(c2,c1)) else NO_CONTAINMENT_VAL
          if(f<minThreshold1Direction && otherVal<minThreshold1Direction){
            edgesToPrune.add( (ds1,ds2,c1,c2))
          }
          if(!otherEdgeExists){
            toAdd.add((ds2,ds1,c2,c1))
          }
        }
        }
      }
    }
    // adding missing edges:
    toAdd.foreach{case (srcDs,targetDs,srcCol,targetCol) => {
      val colPairToContainment = adjacencyListGroupedByDS.getOrElseUpdate(srcDs,mutable.HashMap[Int, mutable.Map[ColEdge, Float]]())
        .getOrElseUpdate(targetDs,mutable.HashMap[ColEdge, Float]())
      assert(!colPairToContainment.contains(ColEdge(srcCol,targetCol)))
      colPairToContainment(ColEdge(srcCol,targetCol)) = NO_CONTAINMENT_VAL
    }}
    //pruning superfluous edges:
    logger.debug("Starting to prune graph")
    logger.debug(s"pruning ${edgesToPrune.size*2} edges out of ${adjacencyListGroupedByDS.mapValues(_.mapValues(_.size).values.sum).values.sum}")
    edgesToPrune.foreach{case (srcDs,targetDS,srcCol,targetCol) => {
      removeBothEdges(srcDs,targetDS,srcCol,targetCol)
    }}
  }

  private def assertIntegrity() = {
    adjacencyListGroupedByDSAndCol.foreach{case (k1,edges) => {
      edges.foreach{ case (k2,_) => {
        val a = adjacencyListGroupedByDSAndCol.getOrElse(k2,Map[DatasetColumnVertex, Float]())
        assert(a.contains(k1))
      }}
    }}
  }


  def switchToAdjacencyListGroupedByDSAndCol() = {
    for (ds1 <- adjacencyListGroupedByDS.keySet) {
      for ((ds2, edges) <- adjacencyListGroupedByDS(ds1)) {
        edges.foreach{ case (ColEdge(c1,c2),f) => {
          val map = adjacencyListGroupedByDSAndCol.getOrElseUpdate(DatasetColumnVertex(ds1,c1),mutable.HashMap[DatasetColumnVertex,Float]())
          map.put(DatasetColumnVertex(ds2,c2),f)
        }}
      }
    }
    adjacencyListGroupedByDS.clear()
  }

  private def setEdgeValue(scrDSID: Int, targetDSID: Int, scrColID: Short, targetColID: Short, threshold: Float) = {
    //one-way
    val map = adjacencyListGroupedByDS.getOrElseUpdate(scrDSID,mutable.HashMap())
    .getOrElseUpdate(targetDSID,mutable.HashMap())
    map(ColEdge(scrColID,targetColID)) = threshold
    //second-way
  }

  private def removeBothEdges(srcDs: Int, targetDS: Int, srcCol: Short, targetCol: Short) = {
    removeEdge(srcDs,targetDS,srcCol,targetCol)
    removeEdge(targetDS,srcDs,targetCol,srcCol)
  }

  private def removeEdge(srcDs: Int, targetDS: Int, srcCol: Short, targetCol: Short) = {
    if(adjacencyListGroupedByDS.contains(srcDs)){
      val outerMap = adjacencyListGroupedByDS(srcDs)
      if(outerMap.contains(targetDS)){
        val map = outerMap(targetDS)
        map.remove(ColEdge(srcCol,targetCol))
        if(map.isEmpty) outerMap.remove(targetDS)
        if(outerMap.isEmpty) adjacencyListGroupedByDS.remove(srcDs)
      }
    }
  }

  //to speed up getOrElse calls (avoids object creation)
  private val emptyMap = Map[Int,Map[ColEdge,(Float,Float)]]()
  private val emptyMap2 = Map[ColEdge,Float]()

  private def hasEdge(scrDSID: Int, targetDSID: Int, scrColID: Short, targetColID: Short): Boolean = {
    adjacencyListGroupedByDS.getOrElse(scrDSID,emptyMap).getOrElse(targetDSID,emptyMap2).contains(ColEdge(scrColID,targetColID))
  }

  def groupedByDSRepresentation: Boolean = !adjacencyListGroupedByDS.isEmpty

  def getEdgeValue(scrDSID: Int, targetDSID: Int, scrColID: Short, targetColID: Short) = {
    if(groupedByDSRepresentation)
      adjacencyListGroupedByDS(scrDSID)(targetDSID)(ColEdge(scrColID,targetColID))
    else
      adjacencyListGroupedByDSAndCol(DatasetColumnVertex(scrDSID,scrColID))(DatasetColumnVertex(targetDSID,targetColID))
  }

  val adjacencyListGroupedByDS = mutable.HashMap[Int,mutable.Map[Int,mutable.Map[ColEdge,Float]]]()
  val adjacencyListGroupedByDSAndCol = mutable.HashMap[DatasetColumnVertex,mutable.Map[DatasetColumnVertex,Float]]()

}

case class ColEdge(src:Short, target:Short)

case class DatasetColumnVertex(dsID:Int, colID:Short)

object JoinabilityGraph {

  def readGraphFromGoOutput(path: File,uniquenessThreshold:Float = Float.NegativeInfinity) = {
    val lineIterator = Source.fromFile(path).getLines()
    val graph = new JoinabilityGraph()
    lineIterator.next()
    while(lineIterator.hasNext){
      val tokens = lineIterator.next().split(",")
      assert(tokens.size==6)
      //pr.println("scrDSID,scrColID,targetDSID,targetColID,highestThreshold,highestUniqueness")
      val (scrDSID,scrColID,targetDSID,targetColID,threshold,maxUniqueness) = (tokens(0).toInt,tokens(1).toShort,tokens(2).toInt,tokens(3).toShort,tokens(4).toFloat,tokens(5).toFloat)
      if(maxUniqueness >= uniquenessThreshold){
        assert(!graph.hasEdge(scrDSID,targetDSID,scrColID,targetColID))
        graph.setEdgeValue(scrDSID,targetDSID,scrColID,targetColID,threshold)
      }
    }
    graph.ensureConsistency()
    graph.assertIntegrity()
    graph
  }
}
