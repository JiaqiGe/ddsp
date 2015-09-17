package seqminer

import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashSet}

/**
 * Created by jge on 6/20/15.
 */
class SeqMiner extends Serializable {
  final val DELIMITER = " "
  var minSup = 1.2
  var maxItr = 20.0
  var method = "control"
  var mode = "origin"
  var extProb = 0.8
  /**
   * 1. from <sid, eid, item> to sequences:
   * s = <(itemset:pe)...(itemset:pe)>
   * 2. add an existential probability
   *
   * @param data
   * @return
   */
  def reformatSynthetic(data: RDD[String]): RDD[Sequence] = {

    val sequences: RDD[(String, Iterable[String])] = data.groupBy(x => x.split(DELIMITER)(0))

    val formattedData = sequences.map(x => {
      //sequence: <eid, [(sid, eid, item)...(sid, eid, item)]>
      val sequence: List[(String, Iterable[String])] = x._2.groupBy(y => y.split(DELIMITER)(1))
        .toList.sortBy(_._1.toInt)

      val itemsets = new ArrayBuffer[Itemset]()

      for (x <- sequence) {
        val eid = x._1.toInt
        val sList = x._2
        val items = new ArrayBuffer[String]()
        sList.foreach(s => {
          items.+=(s.split(DELIMITER)(2))
        })
        itemsets.append(new Itemset(eid, items.sorted.toIterable, extProb))
      }
      new Sequence(itemsets.toIterable)
    })

    return formattedData
  }

  /**
   * two cases: k=2 or k != 2
   * add to the head
   * @param freqPatterns
   */
  def selfJoin(freqPatterns: Array[String], k: Int): List[String] = {
    val candidates = new HashSet[String]()

    for (i <- 0 until freqPatterns.length) {
      for (j <- i + 1 until freqPatterns.length) {
        join(candidates, new Pattern(freqPatterns(i)), new Pattern(freqPatterns(j)), k)
      }
    }
    candidates.toList
  }

  def join(candidates: HashSet[String], s1: Pattern, s2: Pattern, k: Int) = {
    if (k == 2) {
      candidates.++=(Pattern.joinItems(s1.toString(), s2.toString()))
    } else if (k > 2) {
      candidates.++=(s1.join(s2))
      candidates.++=(s2.join(s1))
    }
  }

  /**
   * compute frequent items
   * @param sequenceRDD
   * @return
   */
  def computeFrequentItems(sequenceRDD: RDD[Sequence]): RDD[(String, Double)] = {
    val freqItemsRDD = sequenceRDD.flatMap(seq => {
      val itemsets: Iterable[Itemset] = seq.sequence
      val itemProbs = new mutable.HashMap[String, Double]()

      itemsets.foreach(itemset => {
        val prob = itemset.getProb()
        itemset.getItemset().foreach(item => {
          val preProbs = itemProbs.getOrElse(item, 0.0)
          itemProbs.put(item, 1 - (1 - preProbs) * (1 - prob))
        })
      })

      itemProbs.toIterable
    }).reduceByKey(_ + _).filter(_._2 >= minSup)

    return freqItemsRDD
  }


  /**
   * compute k-length frequent patterns in one iteration
   * @param sequenceRDD
   * @param bcCandidates
   * @return
   */
  def computeFrequentPatterns(sequenceRDD: RDD[Sequence], bcCandidates: Broadcast[List[String]]): RDD[(String, Double)] = {
    sequenceRDD.flatMap(sequence => {
      val candidates = bcCandidates.value
      for (candidate <- candidates) yield sequence.support(candidate, mode)
    }).reduceByKey(_ + _).filter(x => x._2 >= minSup && x._2 > 0)
  }


  /**
   * algorithm USPM to mine sequential patterns in uncertain database
   * USPM use dynamic programming to compute the support probability for
   * each candidate pattern in each sequence
   * @param sequenceRDD
   * @param sc
   * @return
   */
  def mineSequentialPattern(sequenceRDD: RDD[Sequence], sc: SparkContext): RDD[(String, Double)] = {

    val freqItemsRDD = computeFrequentItems(sequenceRDD)
    if (freqItemsRDD == null || freqItemsRDD.isEmpty())
      return null
    // add frequent items to all frequent RDD
    var allFreqPatternRDD = freqItemsRDD
    //start computing k-length k>=2 frequent patterns
    var k = 2
    var freqPatternsRDD = freqItemsRDD
    var isFinished = false
    while (k <= maxItr && !isFinished) {
      val freqPatterns = freqPatternsRDD.map(x => x._1).collect()
      if (freqPatterns == null || freqPatterns.isEmpty) {
        isFinished = true
      } else {
        val candidatePatterns: List[String] = selfJoin(freqPatterns, k)
        if (candidatePatterns == null || candidatePatterns.isEmpty) {
          isFinished = true
        } else {
          val bcCandidate: Broadcast[List[String]] = sc.broadcast(candidatePatterns)
          freqPatternsRDD = computeFrequentPatterns(sequenceRDD, bcCandidate)
          allFreqPatternRDD = allFreqPatternRDD.union(freqPatternsRDD)
          k += 1
        }
      }
    }
    allFreqPatternRDD
  }

  def computeFrequentPatternsDDP(dk: VerticalDB): RDD[(String, Double)] = {
    return dk.getDB().flatMap(vs => {
      //vs: one vertical sequence
      vs.patternSupports()
    }).reduceByKey(_ + _).filter(_._2 >= minSup)
  }

  /**
   * initalize the vertical format of the original uncertain sequence datbase
   * @param sequenceRDD
   * @return
   */
  def ddpInitial(sequenceRDD: RDD[Sequence]): VerticalDB = {
    new VerticalDB(sequenceRDD.map(new VerticalSequence(_)))
  }

  def ddpUpdate(dk: VerticalDB, bcCandidates: Broadcast[List[String]]): VerticalDB = {
    return dk.update(bcCandidates)
  }

  /**
   * algorithm ddpUSPM to mine sequential patterns in uncertain database
   * different from the basic USPM, ddUSPM tries to save intermediate results for each dynamic programming
   * @param sequenceRDD
   * @param sc
   * @return
   */
  def mineSequentialPatternDDP(sequenceRDD: RDD[Sequence], sc: SparkContext): RDD[(String, Double)] = {

    //1-length
    val d1: VerticalDB = ddpInitial(sequenceRDD)

    val frequentPatternsRDD: RDD[(String, Double)] = computeFrequentPatternsDDP(d1)

    var frequentPatterns = frequentPatternsRDD.map(_._1).collect()

    if (frequentPatterns.isEmpty) {
      return frequentPatternsRDD
    }

    //an RDD of all frequent patterns
    var allFreqPatternRDD = frequentPatternsRDD

    var k = 2
    var done = false
    var dkMinusOne: VerticalDB = d1
    while (k <= maxItr && !done) {
      //generate c_k
      val candidatePatterns = selfJoin(frequentPatterns, k)

      if (candidatePatterns.isEmpty) {
        done = true
      } else {
        //broadcast c_{k}
        val bcKCandidates: Broadcast[List[String]] = sc.broadcast(candidatePatterns)
        val dk = ddpUpdate(dkMinusOne, bcKCandidates)
        val kFreqPatternsRDD = computeFrequentPatternsDDP(dk)
        dkMinusOne = dk

        frequentPatterns = kFreqPatternsRDD.map(_._1).collect
        if (frequentPatterns.isEmpty) {
          done = true
        } else {
          allFreqPatternRDD = allFreqPatternRDD.union(kFreqPatternsRDD)
        }
      }
      k += 1
    }

    return allFreqPatternRDD

  }


}

object SeqMiner {

  val logger = Logger.getLogger(this.getClass)
  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    //todo: write to a function instead of main

    if (args == null || args.length < 2) {
      logger.error("not enough parameters!\n Usage:<inputPath><outputPath>")
      System.exit(-1)
    }

    val input:String = args(0)
    val output:String = args(1)
    var itemProb:Double = 0.0
    var numSplit = 10

    val seqMiner = new SeqMiner
    for (i <- 2 until args.length) {
      val opt = args(i)

      if (opt.startsWith("--minSup=")) {
        seqMiner.minSup = opt.stripPrefix("--minSup=").toDouble
      }

      if (opt.startsWith("--numSplit=")) {
        numSplit = opt.stripPrefix("--numSplit=").toInt
      }

      if (opt.startsWith("--maxItr=")) {
        seqMiner.maxItr = opt.stripPrefix("--maxItr=").toDouble
      }

      if (opt.startsWith("--extProb=")) {
        seqMiner.extProb = opt.stripPrefix("--extProb=").toDouble
        itemProb = opt.stripPrefix("--extProb=").toDouble
      }


      if (opt.startsWith("--method=")) {
        if (opt.stripPrefix("--method=").equals("control")) {
          seqMiner.method = "control"
        } else if (opt.stripPrefix("--method=").equals("test")) {
          seqMiner.method = "test"
        } else if (opt.stripPrefix("--method=").equals("data")) {
          seqMiner.method = "data"
        }
        else {
          logger.error("Method name error!")
          System.exit(-1)
        }
      }
    }


    val conf = new SparkConf()
      .setAppName("seqminer")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val textData = sc.textFile(input, numSplit)

    val sequenceRDD: RDD[Sequence] = textData.map(line => {
      val itemsets = new ArrayBuffer[Itemset]()

      for(itemsetRepl <- line.split(";")){
          val eventRepl = itemsetRepl.split(":")
          val eid = eventRepl(0).toInt
          val items = eventRepl(1).split(",")
          itemsets.append(new Itemset(eid, items.sorted.toIterable, 0.8))
      }
      new Sequence(itemsets.toIterable)
    })

//    val allFreqPatternRddDDP = seqMiner.mineSequentialPatternDDP(sequenceRDD, sc)
//    allFreqPatternRddDDP.repartition(1).saveAsTextFile(output + "_test")


    if (seqMiner.method.equals("control")) {
      val allFreqPatternRDD = seqMiner.mineSequentialPattern(sequenceRDD, sc)
      allFreqPatternRDD.repartition(1).saveAsTextFile(output + "_control")
    } else if (seqMiner.method.equals("test")) {
      val allFreqPatternRddDDP = seqMiner.mineSequentialPatternDDP(sequenceRDD, sc)
      allFreqPatternRddDDP.repartition(1).saveAsTextFile(output + "_test")

    } else if (seqMiner.method.equals("data")) {
      sequenceRDD.saveAsObjectFile(output + "data")
    }
  }
}