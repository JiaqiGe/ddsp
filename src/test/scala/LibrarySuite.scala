/*
 * This Scala Testsuite was auto generated by running 'gradle init --type scala-library'
 * by 'jge' at '7/26/15 3:12 PM' with Gradle 2.4
 *
 * @author jge, @date 7/26/15 3:12 PM
 */

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LibrarySuite extends FunSuite {
  test("someLibraryMethod is always true") {
    def library = new Library()
    assert(library.someLibraryMethod)
  }
}
