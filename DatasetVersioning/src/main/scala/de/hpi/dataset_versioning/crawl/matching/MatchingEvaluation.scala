package de.hpi.dataset_versioning.crawl.matching

case class MatchingEvaluation(result:Map[Dataset,Dataset],groundTruth:Map[Dataset,Dataset]) {

  def getConfusionMatrix = {
    var tp,fp,tn,fn = 0
    result.keySet.union(groundTruth.keySet).foreach(ds => {
      val res = result.getOrElse(ds,null)
      val gt = groundTruth.getOrElse(ds,null)
      if(res==gt && res !=null) tp+=1 //correct match
      else if(res==gt && res ==null) tn+=1 //correct non-match
      else if(res!=gt && res !=null) fp+=1 //incorrect match
      else if(res!=gt && res==null) fn+=1 //incorrect non-match
      else throw new AssertionError("unexpected case")
    })
    ConfusionMatrix(tp,fp,tn,fn)
  }

  val confusionMatrix = getConfusionMatrix

  case class ConfusionMatrix(tp: Int, fp: Int, tn: Int, fn: Int) {

    def sum = tp+fp+tn+fn

    def accuracy: Double = (tp+tn) / sum.toDouble
  }

}
