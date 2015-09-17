package seqminer

import scala.collection.mutable
import scala.collection.mutable.HashSet

/**
 * Created by jge on 6/20/15.
 */
class Itemset(val eid:Int, val items:Iterable[String], val prob:Double) extends  Serializable{

  def getEid() = {
    eid
  }

  def getItemset():Iterable[String] = {
    items
  }


  def getProb():Double={
    prob
  }

  def toSetRepl():mutable.HashSet[String] = {
    val items = getItemset()

    var set = new HashSet[String]()
    items.foreach(set += _)

    set
  }

  /**
   *
   * @param s:itemset in a pattern
   * @return : pe if s \subseteq e; otherwise, 0
   */
   def subsetProb(s:String):Double = {
    val eventSet:mutable.HashSet[String] = this.toSetRepl()

    var patSet = new HashSet[String]()

    s.split(",").foreach(patSet += _)

    if(patSet.subsetOf(eventSet)){
      return this.getProb()
    }
    0
  }

}
