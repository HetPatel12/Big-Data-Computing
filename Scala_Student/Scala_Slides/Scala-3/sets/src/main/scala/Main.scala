
abstract class IntSet{
  def incl (x: Int): IntSet // returns the existing set with x added
  def contains (x: Int): Boolean // checks if x is in the existing set
  def union(s: IntSet): IntSet // returns the union of the existing set and the given set s
}

class Empty() extends IntSet{
  def contains(x: Int): Boolean = false
  def incl(x: Int): IntSet = new NonEmpty(x, new Empty(), new Empty())
  def union(s: IntSet): IntSet = s
}

class NonEmpty(elem: Int, left: IntSet, right: IntSet) extends IntSet{
  def contains(x: Int): Boolean =
    if (x < elem)
      left.contains(x)
    else
      if (x > elem)
        right.contains(x)
    else
        true

  def incl(x: Int): IntSet =
    if (x < elem)
      new NonEmpty(elem, left.incl(x), right)
    else
      if (x > elem)
        new NonEmpty(elem, left, right.incl(x))
    else
        this

  def union(s: IntSet): IntSet =
    left.union(right).union(s).incl(elem)

}

class Main {

}
