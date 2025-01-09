import scala.annotation.tailrec

object Main extends App {
  def factorial(n: Int): Int = {
    @tailrec
    def loop(n: Int, acc: Int): Int = {
      if (n == 0)
        acc
      else
        loop(n - 1, acc * n)
    }

    loop(n, 1)
  }

  assert(factorial(0) == 1)
  assert(factorial(1) == 1)
  assert(factorial(2) == 2)
  assert(factorial(5) == 120)
  println("Passed all tests!")
}
