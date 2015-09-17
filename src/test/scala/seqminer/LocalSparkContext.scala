package seqminer

/**
 * Created by jge on 6/21/15.
 */
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, Suite}

trait LocalSparkContext extends BeforeAndAfterAll { self: Suite =>
  @transient var sc: SparkContext = _

  override def beforeAll() {
    sc = new SparkContext("local", "test")
    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    System.clearProperty("spark.driver.port")
    super.afterAll()
  }
}
