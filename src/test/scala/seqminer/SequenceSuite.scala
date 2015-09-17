package seqminer

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}


@RunWith(classOf[JUnitRunner])
class SequenceSuite extends FunSuite with BeforeAndAfter {
  var seq: Sequence = _

  before {
    val itemset1 = new Itemset(1, "A,B,C".split(","), 0.4)
    val itemset2 = new Itemset(2, Array("A", "C"), 0.8)
    val itemset3 = new Itemset(3, Array("B", "D"), 0.5)

    seq = new Sequence(Array(itemset1, itemset2, itemset3))
  }

  test("test computing subset probability") {
    val s1 = "A,B"
    val s2 = "D"
    val itemset = new Itemset(1, "A,B,C".split(","), 0.4)

    assert(Math.abs(itemset.subsetProb(s1) - 0.4) < 0.001)
    assert(itemset.subsetProb(s2) == 0)
  }

  test("test support probability") {
    val s1 = "D"
    assert(Math.abs(seq.support(s1)._2 - 0.5) < 0.001)

    val s2 = "C|D"
    assert(Math.abs(seq.support(s2)._2 - 0.44) < 0.001)

    val s3 = "B,C"
    assert(doubleEquals(seq.support(s3)._2, 0.4))

    val s4 = "A|C|D"
    assert(doubleEquals(seq.support(s4)._2, 0.16))

    val s5 = "A,B|C"
    assert(doubleEquals(seq.support(s5)._2, 0.32))

    val s6 = "E|A"
    assert(doubleEquals(seq.support(s6)._2, 0))

  }

  private def doubleEquals(d1: Double, d2: Double): Boolean = {
    return Math.abs(d1 - d2) < 0.001
  }
}
