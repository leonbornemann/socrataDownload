package de.hpi.dataset_versioning.experiment.example_query_imputation

import java.io.File
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.IdentifierMapping
import de.hpi.dataset_versioning.experiment.{JoinabilityGraph, JoinabilityGraphExplorer}
import de.hpi.dataset_versioning.io.IOService

class ProjectionFinder(version:LocalDate) extends StrictLogging{

  val explorer = new JoinabilityGraphExplorer
  val numericIdToDS = IdentifierMapping.readDatasetIDMapping_intToString(IOService.getDatasetIdMappingFile(version))
  val numericIdToCol = IdentifierMapping.readDatasetIDMapping_intToString(IOService.getColumnIdMappingFile(version))
  val graph = JoinabilityGraph.readGraphFromGoOutput(IOService.getJoinabilityGraphFile(version))

  def find(version: LocalDate) = {
    IOService.cacheMetadata(version)
    val stringIdToMetadata = IOService.cachedMetadata(version)
    //TODO: testing the graph for consistency
    val dsToProjectionCandidates = graph.adjacencyList.keySet.map( dsID => {
      val strID = numericIdToDS(dsID)
      val ncols = stringIdToMetadata(strID).resource.columns_name.size //TODO: this is probably inaccurate - but fast
      val maptoOtherdatasets = graph.adjacencyList(dsID)
      val sortedProjectionCandidates = maptoOtherdatasets.toIndexedSeq.map{case (id,edges) => (id,edges.filter(_._2==1.0))}.sortBy{-_._2.size}
      val highestLength = sortedProjectionCandidates.head._2.size
      val bestCandidates = sortedProjectionCandidates.takeWhile(_._2.size==highestLength)
      //second round of filtering:
      logger.debug(s"For $strID found ${bestCandidates.size} projection candidates with $highestLength columns contained (ncols of this dataset according to metadata: $ncols)")
      (dsID,ncols,highestLength,bestCandidates)
    }).toIndexedSeq
    val sortedProjectionCandidates = dsToProjectionCandidates
      .filter{case (_,_,numColsOverlap,bestCandidates) => numColsOverlap>0 && !bestCandidates.isEmpty}
      .sortBy{case (_,ncol,numColsOverlap,_) => - numColsOverlap / ncol.toFloat}
    sortedProjectionCandidates.take(100)
      .foreach{ case (dsID,ncols,ncolsOverlap,bestCandidates) => {
        val strID = numericIdToDS(dsID)
        logger.debug(s"Found ${bestCandidates.size} Projection Candidates for $strID (overlap detected in $ncolsOverlap out of $ncols):")
        bestCandidates.foreach(c => {
          val strIDCandidate = numericIdToDS(c._1)
          logger.debug(s"\t$strIDCandidate")
        })
      }}
    /*val metaDataList = stringIdToMetadata.toIndexedSeq
    val candidates = scala.collection.mutable.ArrayBuffer[(String,String)]()
    for(i <- 0 until metaDataList.size){
      val (id1,md1) = metaDataList(i)
      for(j <- i until metaDataList.size){
        val (id2,md2) = metaDataList(j)
        val colset1 = md1.resource.columns_name.toSet
        val colset2 = md2.resource.columns_name.toSet
        if(colset1.subsetOf(colset2)){
          candidates.append((id2,id1))
        } else if (colset2.subsetOf(colset1)) {
          candidates.append((id1,id2))
        }
      }
      logger.debug(s"Finished outer loop iteration $i, found ${candidates.size} candidates")
    }*/
  }

}
