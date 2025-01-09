
object Main extends App {
  def mergeSort[T](xs: List[T])(lt: (T, T) => Boolean): List[T] = {
    def merge(xs: List[T], ys: List[T]): List[T] = (xs, ys) match {
      case (Nil, ys) => ys
      case (xs, Nil) => xs
      case (x :: xs1, y :: ys1) =>
        if (lt(x, y))
          x :: merge(xs1, ys)
        else
          y :: merge(xs, ys1)
    }

    val n = xs.length / 2
    if (n == 0)
      xs
    else {
      val (fst, snd) = xs.splitAt(n)
      merge(mergeSort(fst)(lt), mergeSort(snd)(lt))
    }
  }

  val list = List(-5, 6, 3, 2, 7)
  val fruits = List("apple", "pear", "orange", "banana")

  println(s"Sorted numbers: ${mergeSort(list)((x: Int, y: Int) => x < y)}")
  println(s"Sorted fruits: ${mergeSort(fruits)((x: String, y: String) => x.compareTo(y) < 0)}")
}
