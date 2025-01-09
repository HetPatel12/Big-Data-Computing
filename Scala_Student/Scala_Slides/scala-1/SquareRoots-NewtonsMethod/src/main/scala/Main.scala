import scala.annotation.tailrec
import scala.math.abs

object Main extends App{

  /** Calculates the square root of parameter x */
  def sqrt(x: Double): Double = {
    def improve(guess: Double, x: Double): Double = (guess + x / guess) / 2

    def isCloseEnough(guess: Double, x: Double) = abs(guess * guess - x) < 0.001

    def sqrtIteration(guess: Double, x: Double): Double =
      if (isCloseEnough(guess, x))
        guess
      else
        sqrtIteration(improve(guess, x), x)

    sqrtIteration(1.0, x)
  }

  println(sqrt(2))
}
