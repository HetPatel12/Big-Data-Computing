package funsets

/**
 * This class is a test suite for the methods in object FunSets.
 *
 * To run this test suite, start "sbt" then run the "test" command.
 */
class FunSetSuite extends munit.FunSuite:

  import FunSets.*

  test("contains is implemented") {
    assert(contains(x => true, 100))
  }

  /**
   * When writing tests, one would often like to re-use certain values for multiple
   * tests. For instance, we would like to create an Int-set and have multiple test
   * about it.
   *
   * Instead of copy-pasting the code for creating the set into every test, we can
   * store it in the test class using a val:
   *
   *   val s1 = singletonSet(1)
   *
   * However, what happens if the method "singletonSet" has a bug and crashes? Then
   * the test methods are not even executed, because creating an instance of the
   * test class fails!
   *
   * Therefore, we put the shared values into a separate trait (traits are like
   * abstract classes), and create an instance inside each test method.
   *
   */

  trait TestSets:
    val s1 = singletonSet(1)
    val s2 = singletonSet(2)
    val s3 = singletonSet(3)

  /**
   * This test is currently disabled (by using @Ignore) because the method
   * "singletonSet" is not yet implemented and the test would fail.
   *
   * Once you finish your implementation of "singletonSet", remove the
   * .ignore annotation.
   */
  test("singleton set one contains one") {

    /**
     * We create a new instance of the "TestSets" trait, this gives us access
     * to the values "s1" to "s3".
     */
    new TestSets:
      /**
       * The string argument of "assert" is a message that is printed in case
       * the test fails. This helps identifying which assertion failed.
       */
      assert(contains(s1, 1), "Singleton 1")
      assert(!contains(s1, 2), "Singleton 2")
      assert(contains(s2, 2), "Singleton 3")
      assert(contains(s3, 3), "Singleton 4")
  }

  test("union contains all elements of each set") {
    new TestSets:
      val s = union(s1, s2)
      assert(contains(s, 1), "Union 1")
      assert(contains(s, 2), "Union 2")
      assert(!contains(s, 3), "Union 3")
  }

  test("intersection contains all elements that exist in both sets") {
    new TestSets:
      val s = intersect(s1, s3)
      assert(!contains(s, 1), "Intersect 1")
      assert(!contains(s, 3), "Intersect 2")
      var s4 = singletonSet(1)
      val i = intersect(s1, s4)
      assert(contains(i, 1), "Intersect 3")
  }

  test("difference contains all elements that exist in one set, but don't exist in the other") {
    new TestSets:
      val s = diff(s1, s3)
      assert(contains(s, 1), "Difference 1")
      assert(!contains(s, 3), "Difference 2")
      var s4 = singletonSet(1)
      val d = diff(s1, s4)
      assert(!contains(d, 1), "Difference 3")
  }

  test("fiter contains all elements that hold the p function") {
    new TestSets:
      var s4 = singletonSet(-1)
      val f1 = filter(s1, x => x > 0)
      val f2 = filter(s4, x => x > 0)
      val f3 = filter(s4, x => x < 0)
      val f4 = filter(s3, x => x % 3 == 0)
      assert(contains(f1, 1), "Filter 1")
      assert(!contains(f2, -1), "Filter 2")
      assert(contains(f3, -1), "Filter 3")
      assert(contains(f4, 3), "Filter 4")
  }

  test("forall verifies that all elements hold the p function") {
    new TestSets:
      var s4 = union(s3, union(s1, s2))
      assert(forall(s1, x => x > 0), "Forall 1")
      assert(forall(s4, x => x > 0), "Forall 2")
      assert(!forall(s4, x => x % 2 == 0), "Forall 3")
      assert(forall(s1, x => x == x * x), "Forall 4")
  }

  test("exists verifies that at least one element holds the p function") {
    new TestSets:
      var s4 = union(s3, union(s1, s2))
      assert(exists(s1, x => x > 0), "Forall 1")
      assert(!exists(s4, x => x < 0), "Forall 2")
      assert(exists(s4, x => x % 2 == 0), "Forall 3")
      assert(exists(s4, x => x == x * x), "Forall 4")
  }

  test("map transforms all elements using p function") {
    new TestSets:
      var s = union(s3, union(s1, s2))
      var m1 = map(s, x => x * 2)
      var m2 = map(s, x => x * x)
      assert(contains(m1, 4), "Map 1")
      assert(!contains(m1, 1), "Map 2")
      assert(contains(m2, 1), "Map 3")
      assert(!contains(m2, 2), "Map 4")
  }

  import scala.concurrent.duration.*
  override val munitTimeout = 10.seconds