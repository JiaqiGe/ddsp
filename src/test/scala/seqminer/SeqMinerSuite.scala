package seqminer

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

/**
 * Created by jge on 6/21/15.
 */

abstract class UnitSpec extends FunSuite with Matchers with
OptionValues with Inside with Inspectors with LocalSparkContext
with BeforeAndAfter with Serializable


@RunWith(classOf[JUnitRunner])
class SeqMinerSuite extends UnitSpec {
  var dataRDD: RDD[String] = _
  var seqMiner: SeqMiner = _
  before {
    val data = Array("1 1 A", "1 1 B", "1 2 C", "1 2 A", "1 3 D", "2 1 A", "2 1 B", "2 2 C", "2 3 D")
    dataRDD = sc.parallelize(data, 2)
    seqMiner = new SeqMiner
  }

  test("data reformat") {
    val s1 = seqMiner.reformatSynthetic(dataRDD).collect()(1).sequence.toList

    val itemsets1 = s1(0).getItemset().toList
    val itemsets2 = s1(1).getItemset().toList

    assert(itemsets1(0) == "A" && itemsets1(1) == "B")
    assert(itemsets2(0) == "A" && itemsets2(1) == "C")
  }

  test("self join to generate candidates") {
    val freqPat1 = Array("a", "b")
    val freqPat2 = Array("b|c", "a|b", "a,b")

    var cands = seqMiner.selfJoin(freqPat1, 2)

    assert(cands.size == 3)
    assert(cands.contains("b|a"))
    assert(cands.contains("a|b"))
    assert(cands.contains("a,b"))

    cands = seqMiner.selfJoin(freqPat2, 3)

    assert(cands.contains("a|b|c"))
    assert(cands.contains("a,b|c"))
  }

  test("compute frequent items") {
    seqMiner.minSup = 1
    seqMiner.extProb = 0.5
    val sequenceRDD = seqMiner.reformatSynthetic(dataRDD)
    val itemRDD: RDD[(String, Double)] = seqMiner.computeFrequentItems(sequenceRDD)
    val freqItems = itemRDD.collect().toList.sortBy(x => x._1)

    assertResult(4)(freqItems.size)
    assertResult("A")(freqItems(0)._1)
    assert(Math.abs(freqItems(0)._2 - 1.25) < 0.001)

    assertResult("B")(freqItems(1)._1)
    assert(Math.abs(freqItems(1)._2 - 1.0) < 0.001)
  }

  test("compute frequent patterns by ddp") {
    seqMiner.minSup = 0
    seqMiner.extProb = 0.5
    val sequenceRDD: RDD[Sequence] = seqMiner.reformatSynthetic(dataRDD)

    val frequentPatterns = seqMiner.mineSequentialPatternDDP(sequenceRDD, sc).collect().toMap

    assertResult(25)(frequentPatterns.size)
    assertResult(frequentPatterns("A,B"))(1.0)
  }

  test("compute frequent patterns") {
    seqMiner.minSup = 1
    seqMiner.extProb = 0.5
    val sequenceRDD: RDD[Sequence] = seqMiner.reformatSynthetic(dataRDD)
    val itemRDD: RDD[(String, Double)] = seqMiner.computeFrequentItems(sequenceRDD)
    val items = itemRDD.collect.map(_._1)
    val candidatePatterns = seqMiner.selfJoin(items, 2)

    val bcCandidate: Broadcast[List[String]] = sc.broadcast(candidatePatterns)

    val frequentPatterns = seqMiner.computeFrequentPatterns(sequenceRDD, bcCandidate).collect.toMap

    assertResult(1)(frequentPatterns.size)
    assertResult(1.0)(frequentPatterns("A,B"))
  }

  test("compute all frequent patterns") {
    seqMiner.minSup = 0
    seqMiner.extProb = 0.5
    val sequenceRDD: RDD[Sequence] = seqMiner.reformatSynthetic(dataRDD)
    val allFreqPattern = seqMiner.mineSequentialPattern(sequenceRDD, sc).collect

    assertResult(25)(allFreqPattern.size)
  }

  test("test optimized algorithm") {
    seqMiner.minSup = 0.3
    seqMiner.extProb = 0.5
    seqMiner.mode = "origin"
    val sequenceRDD: RDD[Sequence] = seqMiner.reformatSynthetic(dataRDD)
    val pattern1 = seqMiner.mineSequentialPattern(sequenceRDD, sc).collect.sortBy(_._1)

    seqMiner.mode = "optimize"
    val pattern2 = seqMiner.mineSequentialPattern(sequenceRDD, sc).collect.sortBy(_._1)

    assert(pattern1.size == pattern2.size)
    assert(pattern1(0)._1 == pattern2(0)._1)


  }




}


