package recfun

import scala.annotation.tailrec

object RecFun extends RecFunInterface:

  def main(args: Array[String]): Unit =
    println("Pascal's Triangle")
    for row <- 0 to 10 do
      for col <- 0 to row do
        print(s"${pascal(col, row)} ")
      println()

  /**
   * Exercise 1 - Pascal's Triangle
   */
  def pascal(c: Int, r: Int): Int = {
    def nextRow(prevLine: List[Int]): List[Int] = {
      val pairs = prevLine.zip(prevLine.drop(1).appended(0))
      pairs.foldLeft(List[Int](1)){
        case (xs, (x, y)) => xs.appended(x + y)}
    }

    @tailrec
    def loop(column: Int, row: Int, prevLine: List[Int], currentLine: List[Int]): Int = {
      // then check if it is the right one, otherwise move to the next element
      if (column == c && row == r)
        currentLine(c)
      else
        if (column == row)
          loop(0, row + 1, currentLine, nextRow(currentLine))
        else
          loop(column + 1, row, prevLine, currentLine)
    }

    loop(0, 0, List[Int](1), List[Int](1))
  }

  /**
   * Exercise 2 - Parentheses Balancing
   */
  def balance(chars: List[Char]): Boolean = {
    @tailrec
    def loop(characters: List[Char], acc: Int): Boolean = characters match {
      case Nil => acc == 0
      case _ if acc < 0 => false
      case '(' :: tail => loop(tail, acc + 1)
      case ')' :: tail => loop(tail, acc - 1)
      case _ :: tail => loop(tail, acc)
    }

    loop(chars, 0)
  }

  /**
   * Exercise 3 - Counting Change
   */
  def countChange(money: Int, coins: List[Int]): Int = {
    @tailrec
    def loop(coinIndex: Int, sumIndex: Int, dp: List[Int]): Int = {
      // dp[index] = number of ways to change "index" money
       if (coinIndex == coins.length)
         dp(money)
       else
         if (sumIndex == money + 1)
          loop(coinIndex + 1, 0, dp)
         else
           if (coins(coinIndex) <= sumIndex)
             loop(coinIndex, sumIndex + 1, dp.updated(sumIndex, dp(sumIndex) + dp(sumIndex - coins(coinIndex))))
           else
             loop(coinIndex, sumIndex + 1, dp)
    }

    loop(0, 0, List(1) ++ List.fill[Int](money)(0))
  }
