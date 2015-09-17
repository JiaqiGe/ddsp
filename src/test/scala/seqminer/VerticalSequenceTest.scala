package seqminer

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import seqminer._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by jge on 9/7/15.
 */

@RunWith(classOf[JUnitRunner])
class VerticalSequenceTest extends FunSuite with BeforeAndAfter {
  var seq: Sequence = _
  var vs: VerticalSequence = _

  before {
    val itemset1 = new Itemset(1, "A,B,C".split(","), 0.4)
    val itemset2 = new Itemset(2, Array("A", "C"), 0.8)
    val itemset3 = new Itemset(3, Array("B", "D"), 0.5)

    seq = new Sequence(Array(itemset1, itemset2, itemset3))
    vs = new VerticalSequence(seq)
  }

  test(("test vertical sequence initialization")) {
    assertResult(vs.events.size())(seq.size())
    assertResult(vs.events(0).getEid())(1)
    assertResult(vs.events(1).getEid())(2)
    assertResult(vs.patternOccurrences.size())(4)
    val occA: ArrayBuffer[Occurrence] = vs.patternOccurrences.getOccurrences(new Pattern("A"))
    assertResult(occA.size)(2)
    assertResult(occA(0).getSupportProb())(0.8)
    assertResult(occA(1).getSupportProb())(0.88)

    val occB: ArrayBuffer[Occurrence] = vs.patternOccurrences.getOccurrences(new Pattern("B"))
    assertResult(occB(0).getSupportProb())(0.5)
    assertResult(occB(1).getSupportProb())(0.7)
  }

  test("update vertical sequence for sequence extended patterns") {
    val occA: ArrayBuffer[Occurrence] = vs.patternOccurrences.getOccurrences(new Pattern("A"))
    val occB: ArrayBuffer[Occurrence] = vs.patternOccurrences.getOccurrences(new Pattern("B"))
    val occC: ArrayBuffer[Occurrence] = vs.patternOccurrences.getOccurrences(new Pattern("C"))

    val occAB: ArrayBuffer[Occurrence] = vs.seqExtOccurrences(occA, occB)
    assertResult(occAB(0).getSupportProb())(0.4)
    assertResult(occAB(1).getSupportProb())(0.44)

    val occAC = vs.itemExtOccurrences(occA, occC)
    assertResult(occAC(0).getSupportProb())(0.8)
    assertResult(occAC(1).getSupportProb())(0.88)
  }

  test("rebuild dk"){
    val candidates = List("A,C", "A|B")
    val dk:VerticalSequence = vs.build(candidates)
    val supports = dk.patternSupports().toMap

    assertResult(0.88)(supports("A,C"))
    assertResult(0.44)(supports("A|B"))
  }
}
