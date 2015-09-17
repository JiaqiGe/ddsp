package seqminer

import scala.collection.mutable.HashSet

/**
 * Created by jge on 6/21/15.
 */
class Pattern(val pattern:String) extends Serializable{

  def isSequenceExtend(): Boolean = {
    if(pattern.charAt(getFirstIndex()) == '|')
      return true

    return false
  }

  def join(other:Pattern):HashSet[String] = {
    val candidates = new HashSet[String]()

    if(this.removeLastItem().equals(other.removeFirstItem())){
      val toAppend = other.getFirstItemWithDelimiter()
      candidates.+=(toAppend+pattern)
    }
    candidates
  }
  
  def removeLastItem() : String = {
    pattern.substring(0,getLastIndex())
  }


  def removeFirstItem():String = {
    pattern.substring(getFirstIndex()+1)
  }

  def getFirstItemWithDelimiter():String = {
    pattern.substring(0,getFirstIndex()+1)
  }

  def getLastItemWithDelimiter():String = {
    pattern.substring(getLastIndex())
  }


  private def getFirstIndex():Int = {
    var startIndex = 0
    var i = 0
    while(i < pattern.length){
      if(pattern.charAt(i) == ',' || pattern.charAt(i) == '|'){
        startIndex = i
        i = pattern.length
      }else{
        i += 1
      }
    }
    startIndex
  }

  private def getLastIndex():Int = {
    var endIndex = pattern.length
    var i = pattern.length-1

    while(i >= 0){
      if(pattern.charAt(i) == ',' || pattern.charAt(i) == '|'){
        endIndex= i
        i = -1
      }else{
        i -= 1
      }
    }
    endIndex
  }

  override def toString():String = {
    pattern
  }

  override def hashCode():Int = {
    return pattern.hashCode
  }

  def canEqual(a:Any) = a.isInstanceOf[Pattern]

  override def equals(that: Any):Boolean = {
    that match {
      case that: Pattern => that.canEqual(this) && that.hashCode() == this.hashCode()
    }
  }

}

object Pattern{
  def joinItems(item1:String, item2: String): HashSet[String] = {
    val cands = new HashSet[String]()

    if (item1.equals(item2)) {
      return cands
    }

    cands.+=(item1 + "|" + item2)
    cands.+=(item2 + "|" + item1)

    if (item1 < item2) {
      cands.+=(item1 + "," + item2)
    } else if (item1 > item2) {
      cands.+=(item2 + "," + item1)
    }

    cands
  }
}
