package seqminer


/**
 * Created by jge on 6/20/15.
 */
class Sequence(var sequence: Iterable[Itemset]) extends Serializable {

  def size(): Int = {
    sequence.size
  }


  def support(candidate: String, mode: String): (String, Double) = {
    if (mode == "origin")
      return support(candidate)
    else if (mode == "optimize")
      return support2(candidate)
    else {
      throw new NoSuchElementException("no such mode!Please set to 'origin' or 'optimize'")
    }

  }

  /**
   * compute the support probability of a candidate pattern in one sequence
   * @param candidate
   * @return
   */
  def support(candidate: String): (String, Double) = {
    //initialize
    val s = candidate.split("\\|")
    val d = sequence.toList

    val n = s.length
    val m = d.size

    val P = Array.fill[Double](n + 1, m + 1)(0)

    for (i <- 0 to m) {
      P(n)(i) = 1
    }

    for (i <- n - 1 to 0 by -1; j <- m - 1 to 0 by -1) {
      val pe = d(j).subsetProb(s(i))
      P(i)(j) = (1-pe)*P(i)(j + 1) +  pe*P(i + 1)(j + 1)
    }

    (candidate, P(0)(0))
  }

  def support2(candidate: String): (String, Double) = {
    val s = candidate.split("\\|")
    val d = sequence.toList

    val n = s.length
    val m = d.size

    val P = Array.fill[Double](n + 1, m + 1)(0)

    for (i <- 0 to m) {
      P(n)(i) = 1
    }

    for (i <- n - 1 to 0 by -1; j <- m - 1 to 0 by -1) {

      if (i > j) {
        P(i)(j) = 0
      } else if (n - i > m - j) {
        P(i)(j) = 0
      } else {
        val pe = d(j).subsetProb(s(i))
        P(i)(j) = pe*P(i+1)(j+1)+(1-pe)*P(i)(j+1)
      }
    }
    (candidate, P(0)(0))
  }


  private def doubleEquals(d1: Double, d2: Double): Boolean = {
    return Math.abs(d1 - d2) < Double.MinPositiveValue
  }


  def getSequence():Iterable[Itemset]={
    return this.sequence
  }

  /**
   *
   * @return
   */
  def itemCount() = {
    sequence.flatMap(x => x.getItemset().toList).map(x => (x, 1))
  }
}
