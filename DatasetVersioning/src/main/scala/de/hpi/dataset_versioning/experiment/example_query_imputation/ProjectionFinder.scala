package de.hpi.dataset_versioning.experiment.example_query_imputation

import java.io.{File, PrintWriter}
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.metadata.DatasetMetadata
import de.hpi.dataset_versioning.data.{IdentifierMapping, LoadedRelationalDataset}
import de.hpi.dataset_versioning.experiment.{JoinabilityGraph, JoinabilityGraphExplorer}
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

class ProjectionFinder(version:LocalDate) extends StrictLogging{

  val explorer = new JoinabilityGraphExplorer
  val numericIdToDS = IdentifierMapping.readDatasetIDMapping_intToString(IOService.getDatasetIdMappingFile(version))
  val numericIdToCol = IdentifierMapping.readColumnIDMapping_shortToString(IOService.getColumnIdMappingFile(version))
  val graph = JoinabilityGraph.readGraphFromGoOutput(IOService.getJoinabilityGraphFile(version))
  logger.trace("Finished Projection Finder Initialization")

  /***
   * Assumes unchanged column names - which provides the schema mapping!
   * @param originalDS
   * @param projectedDS
   * @return
   */
  def isProjection(originalDS: LoadedRelationalDataset, projectedDS: LoadedRelationalDataset,projectedToOriginalMapping:mutable.HashMap[String,String]): Boolean = {
    //some early aborts:
    if(originalDS.ncols < projectedDS.ncols) false
    else {
      val columnRenames = projectedDS.colNames.map(s => (projectedToOriginalMapping(s),s))
      val projectedFromOriginal = originalDS.getProjection(originalDS.id + "_projected",columnRenames)
      assert(projectedFromOriginal.colNames == projectedDS.colNames)
      projectedFromOriginal.tupleSetEqual(projectedDS)
    }
  }

  def getColumnMapping(projectedDS: LoadedRelationalDataset, originalDS: LoadedRelationalDataset, edges: Set[(String, String)]) = {
    val map = edges.groupBy(_._1)
      .toSeq
      .sortBy(_._2.size)
    val takenColumns = scala.collection.mutable.HashSet[String]()
    val columnMapping = mutable.HashMap[String,String]()
    val a = map.iterator
    var abort = false
    while(a.hasNext && !abort){
      var (srcCol,edges) = a.next()
      val candidateMatches = edges.map(_._2)
        .filter(!takenColumns.contains(_))
      if(candidateMatches.size==0)
        abort = true
      else if(edges.size==1){
        takenColumns += candidateMatches.head
        columnMapping(srcCol) = candidateMatches.head
      } else{
        takenColumns += candidateMatches.head
        columnMapping(srcCol) = candidateMatches.head
        logger.debug("encountered difficult column matching case")
      }
    }
    if(abort){
      None
    } else{
      //try to match the rest of the columns:
      val freeInProjected = projectedDS.colNames.toSet.diff(columnMapping.keySet)
        .map(s => (s,projectedDS.columnHashes(s)))
        .toMap
      if(freeInProjected.isEmpty)
        Some(columnMapping)
      else{
        val freeInOriginal = originalDS.colNames.toSet.diff(columnMapping.values.toSet)
          .map(s => (s,projectedDS.columnHashes(s)))
          .toMap
        //invert the maps:
        val projectedHashToColname = freeInProjected.groupBy(_._2).mapValues(_.keys)
        val originalHashToColname = freeInOriginal.groupBy(_._2).mapValues(_.keys)
        val it = projectedHashToColname.iterator
        while(a.hasNext && !abort){
          val (projectedHash,colNamesProjected) = it.next()
          val matchingColNames = originalHashToColname(projectedHash).toSeq
            .filter(!takenColumns.contains(_))
          if(colNamesProjected.size==matchingColNames.size){
            columnMapping ++= colNamesProjected.zip(matchingColNames)
            takenColumns ++=matchingColNames
          } else{
            abort = true
          }
        }
        if(abort) None
        else Some(columnMapping)
      }
    }
  }

  def find(version: LocalDate) = {
    IOService.cacheMetadata(version)
    val stringIdToMetadata = IOService.cachedMetadata(version)
    //TODO: testing the graph for consistency
    val dsToProjectionCandidates = graph.adjacencyList.keySet.map( dsID => {
      val strID = numericIdToDS(dsID)
      val ncols = stringIdToMetadata(strID).resource.columns_name.size //TODO: this is probably inaccurate - but fast
      val maptoOtherdatasets = graph.adjacencyList(dsID)
      val sortedProjectionCandidates = maptoOtherdatasets.toIndexedSeq.map{case (id,edges) => {
        val newEdges = edges.filter(_._2 == 1.0)
          .map(e => e._1) //we are only interested in how many outgoing edges of distinct columns we have
          .toSet
        (id,newEdges,newEdges.map(_._1).toSet.size)
        }}.sortBy{-_._3}
      val highestLength = sortedProjectionCandidates.head._3
      val bestCandidates = sortedProjectionCandidates.takeWhile(t => t._3==highestLength) //Only highest length can be candidates
      //second round of filtering:
      logger.debug(s"For $strID found ${bestCandidates.size} projection candidates with $highestLength columns contained (ncols of this dataset according to metadata: $ncols)")
      (dsID,ncols,highestLength,bestCandidates)
    }).toIndexedSeq
    logger.trace("Finished Projection Finder Initialization")
    val sortedProjectionCandidates = dsToProjectionCandidates
      .filter{case (_,_,numColsOverlap,bestCandidates) => numColsOverlap>0 && !bestCandidates.isEmpty}
      .sortBy{case (_,ncol,numColsOverlap,_) => - numColsOverlap / Math.max(numColsOverlap,ncol).toFloat}
    /*sortedProjectionCandidates.take(100)
      .foreach{ case (dsID,ncols,ncolsOverlap,bestCandidates) => {
        val strID = numericIdToDS(dsID)
        logger.debug(s"Found ${bestCandidates.size} Projection Candidates for $strID (overlap detected in $ncolsOverlap out of $ncols):")
        bestCandidates.foreach(c => {
          val strIDCandidate = numericIdToDS(c._1)
          logger.debug(s"\t$strIDCandidate")
        })
      }}*/
    logger.debug(s"Going through ${sortedProjectionCandidates.size} candidate lists")
    var checkCount = 0
    var numHits = 0
    val pr = new PrintWriter(IOService.getInferredProjectionFile(version))
    pr.println("originalDataset,projectedDataset,originalNcols,projectedNcols")
    for(i <- 0 until sortedProjectionCandidates.size) {
      val (projectedID,_,_,candidates) = sortedProjectionCandidates(i)
      val projectedDSStringID = numericIdToDS(projectedID)
      val projectedDS = IOService.tryLoadDataset(projectedDSStringID,version)
      projectedDS.columnHashes
      if(!projectedDS.isEmpty){
        for((srcID,edges,columnMatchCount) <- candidates) {
          val srcDSStringID = numericIdToDS(srcID)
          val originalDS = IOService.tryLoadDataset(srcDSStringID,version)
          originalDS.calculateColumnHashes()
          val columnMapping = getColumnMapping(projectedDS,originalDS,edges.map{case (c1,c2) => (numericIdToCol((projectedID,c1)),numericIdToCol((srcID,c2)))})
          if(columnMapping.isDefined && isProjection(originalDS,projectedDS,columnMapping.get)){
            pr.println(s"${originalDS.id},${projectedDS.id},${originalDS.ncols},${projectedDS.ncols}")
            pr.flush()
            numHits +=1
          }
          checkCount +=1
          if(checkCount % 100 ==0){
            logger.debug(s"Found $numHits true projection in $checkCount tries (accuracy ${100*numHits / checkCount.toFloat}%)")
          }
        }
      }
      if(i %100 ==0) logger.debug(s"Processed ${i+1} out of ${sortedProjectionCandidates.size} lists (${100*(i+1)/sortedProjectionCandidates.size.toFloat}%)")
    }
    pr.close()
  }

}
