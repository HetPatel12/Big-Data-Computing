import scala.annotation.tailrec

object Main extends App{
  def queens(n: Int): Set[List[Int]] = {
    def isSafe(col: Int, queens: List[Int]): Boolean = {
      @tailrec
      def checks(col: Int, delta: Int, queens: List[Int]): Boolean = queens match {
        case queenColumn :: others =>
          queenColumn == col || //vertical check
          (queenColumn - col).abs == delta || // diagonal check
          checks(col, delta + 1, others) // move to the next placed queen
        case Nil => false
      }

      !checks(col, 1, queens)
    }

    def placeQueens(k: Int): Set[List[Int]] = {
      if (k == 0)
        Set(List())
      else
        for(
          queens <- placeQueens(k - 1);
          col <- 0 until n
          if isSafe(col, queens)
        )
        yield col :: queens
    }

    placeQueens(n)
  }

  println(queens(4))
  println(queens(8))
}
