package seqminer

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * Created by jge on 9/2/15.
 */
class VerticalSequence(val events: Events, val patternOccurrences: PatternOccurrence) extends Serializable {

  def this(events: Events) = {
    this(events, new PatternOccurrence())
  }

  /**
   * secondary constructor for initialization
   */
  def this(sequence: Sequence) = {
    this(new Events(sequence), new PatternOccurrence())

    //build PatternOccurrences
    init()
  }

  def patternSupports(): TraversableOnce[(String, Double)] = {
    patternOccurrences.getPatternSupportProb()
  }


  def insert(candidate: Pattern, occurrences: ArrayBuffer[Occurrence]): Unit = {
    patternOccurrences.put(candidate, occurrences)
  }


  /**
   * initialize PatternOccurrences from original sequence database
   */
  def init(): Unit = {
    for (i <- (events.size - 1 to 0 by -1)) {
      val event: Event = events(i)

      event.getI().foreach(item => {
        patternOccurrences.init(new Pattern(item), event)
      })
    }
  }

  def seqExtOccurrences(o1: ArrayBuffer[Occurrence], o2: ArrayBuffer[Occurrence]): ArrayBuffer[Occurrence] = {
    val o = new ArrayBuffer[Occurrence]()

    var i1 = 0
    var i2 = 0
    while (i1 < o1.size) {
      // P(s1 \subseteq ej)
      val event = o1(i1).getEvent()
      val firstElemProb = event.getProb()

      //search for P(s' \preceq dj)
      var subSeqProb: Double = 0

      //debug
      val eid2 = o2(i2).getEvent().getEid()
      val eid1 = event.getEid()

      while (i2 < o2.size && o2(i2).getEvent().getEid() > event.getEid()) {
        subSeqProb = o2(i2).getSupportProb()
        i2 += 1
      }
      i2 -= 1

      i2 = Math.max(0, i2)

      var supProb: Double = 0
      if (o.isEmpty) {
        supProb = firstElemProb * subSeqProb
      } else {
        supProb = firstElemProb * subSeqProb + (1 - firstElemProb) * (o.last.getSupportProb())
      }

      //create an occurrence and insert into o
      if (supProb > 0) {
        o.append(new Occurrence(supProb, event))
      }
      i1 += 1
    }

    return o
  }

  def itemExtOccurrences(o1: ArrayBuffer[Occurrence], o2: ArrayBuffer[Occurrence]): ArrayBuffer[Occurrence] = {
    val o = new ArrayBuffer[Occurrence]()

    var i1 = 0
    var i2 = 0
    while (i1 < o1.size && i2 < o2.size) {
      val event = o1(i1).getEvent()
      val eid1 = o1(i1).getEvent().getEid()
      val eid2 = o2(i2).getEvent().getEid()

      if (eid1 == eid2) {
        val firstElemProb = event.getProb()
        val subSeqProb = o2(i2).getSupportProb()
        var subSeqProbNext: Double = 0
        if (i2 - 1 >= 0) {
          subSeqProbNext = o2(i2 - 1).getSupportProb()
        }
        var supProbNext: Double = 0
        if (!o.isEmpty) {
          supProbNext = o.last.getSupportProb()
        }
        val supProb = (1 - firstElemProb) * (supProbNext - subSeqProbNext) + subSeqProb
        o.append(new Occurrence(supProb, event))
        i1 += 1
        i2 += 1
      } else if (eid1 < eid2) {
        i2 += 1
      } else {
        i1 += 1
      }
    }
    return o
  }

  def buildPatternOccurrences(o1: ArrayBuffer[Occurrence], o2: ArrayBuffer[Occurrence], isSequenceExtend: Boolean): ArrayBuffer[Occurrence] = {
    if (isSequenceExtend) {
      return seqExtOccurrences(o1, o2)
    } else {
      return itemExtOccurrences(o1, o2)
    }
  }

  /**
   * update vertical sequence by k+1 length candidates
   * @param candidates: a list of k+1 lenth candidates
   */
  def build(candidates: List[String]): VerticalSequence = {
    val dk = new VerticalSequence(this.events)

    candidates.foreach(candidateStr => {

      val candidate = new Pattern(candidateStr)
      val s1 = candidate.removeLastItem()
      val s2 = candidate.removeFirstItem()
      val isSequenceExtend = candidate.isSequenceExtend()

      if (patternOccurrences.containPattern(s1) && patternOccurrences.containPattern(s2)) {
        //update
        val occurrences1 = patternOccurrences.getOccurrences(new Pattern(s1))
        val occurrences2 = patternOccurrences.getOccurrences(new Pattern(s2))

        val candidateOccurrences: ArrayBuffer[Occurrence] = buildPatternOccurrences(occurrences1, occurrences2, isSequenceExtend)

        if (candidateOccurrences != null && !candidateOccurrences.isEmpty)
          dk.insert(candidate, candidateOccurrences)
      }
    })

    return dk
  }


}


/**
 * a class of pattern occurrences
 * @param patternOccurrence
 */
class PatternOccurrence(val patternOccurrence: mutable.Map[Pattern, ArrayBuffer[Occurrence]]) extends Serializable {

  def this() {
    this(new mutable.HashMap[Pattern, ArrayBuffer[Occurrence]]())
  }

  def size(): Int = {
    patternOccurrence.size
  }

  def getPatternSupportProb(): TraversableOnce[(String, Double)] = {
    return patternOccurrence.map { case (pattern: Pattern, occurs: ArrayBuffer[Occurrence]) =>
      (pattern.toString(), occurs.last.getSupportProb())
    }.toTraversable
  }

  def getOccurrences(p: Pattern): ArrayBuffer[Occurrence] = {
    return patternOccurrence.get(p).get
  }

  def containPattern(s: String): Boolean = {
    return containPattern(new Pattern(s))
  }

  def containPattern(p: Pattern): Boolean = {
    return patternOccurrence.contains(p)
  }

  def put(pattern: Pattern, occurrences: ArrayBuffer[Occurrence]): Unit = {
    patternOccurrence.put(pattern, occurrences)
  }


  /**
   * init data structure for a 1-lenth patterns
   * @param pattern
   * @param event
   */
  def init(pattern: Pattern, event: Event): Unit = {
    if (patternOccurrence.contains(pattern)) {
      //insert & update
      val preOccurrences = patternOccurrence.get(pattern).get
      val prob = event.getProb() + (1 - event.getProb()) * preOccurrences.last.supProb
      val newOccurrence = new Occurrence(prob, event)
      preOccurrences.append(newOccurrence)

      patternOccurrence.put(pattern, preOccurrences)
    } else {
      //create
      val occurrences = new ArrayBuffer[Occurrence]()
      //first occurrence of an item
      val firstOccurrence = new Occurrence(event.getProb(), event)
      occurrences.append(firstOccurrence)
      patternOccurrence.put(pattern, occurrences)
    }
  }
}

class Occurrence(val supProb: Double, val event: Event) extends Serializable {

  def getSupportProb(): Double = {
    return supProb
  }

  def getEvent(): Event = {
    return event
  }

  def getItemProb(): Double = {
    return event.getProb()
  }


}

/**
 *
 * @param events
 */
class Events(var events: List[Event]) extends Serializable {

  def this() {
    this(new ArrayBuffer[Event]().toList)
  }

  def this(sequence: Sequence) {
    this()
    this.events = sequence.getSequence().map { uItemset =>
      new Event(uItemset.getEid(), uItemset.getItemset(), uItemset.getProb())
    }.toList
  }


  def size(): Int = {
    return events.size
  }

  def apply(i: Int): Event = {
    return events(i)
  }

}

class Event(val eid: Int, val I: Iterable[String], val prob: Double) {
  def getProb(): Double = {
    return prob
  }


  def getI(): Iterable[String] = {
    return I
  }


  def getEid(): Int = {
    return eid
  }
}