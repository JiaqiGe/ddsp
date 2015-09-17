package seqminer

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * Created by jge on 9/1/15.
 */
class VerticalDB(val db: RDD[VerticalSequence]) extends Serializable {

  def getDB():RDD[VerticalSequence] = {
    db
  }


  //update db by candidates
  def update(candidateBR: Broadcast[List[String]]) = {
    new VerticalDB(db.map(_.build(candidateBR.value)))
  }
}
