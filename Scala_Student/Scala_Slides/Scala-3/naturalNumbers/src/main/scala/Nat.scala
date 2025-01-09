abstract class Nat {
  def isZero: Boolean
  def predecessor: Nat
  def successor: Nat
  def + (that: Nat): Nat
  def - (that: Nat): Nat
}

object Zero extends Nat{
  def isZero: Boolean = true

  def predecessor: Nat = throw new Exception("Zero has no natural predecessor")

  def successor: Nat = new Successor(this)

  def +(that: Nat): Nat = that

  def -(that: Nat): Nat =
    if (that.isZero)
      this
    else
      throw new Exception(s"$that cannot be subtracted from zero")

  override def toString: String = "Zero"
}

class Successor(nat: Nat) extends Nat{
  override def isZero: Boolean = false

  override def predecessor: Nat = nat

  override def successor: Nat = new Successor(this)

  override def +(that: Nat): Nat = new Successor(nat + that)

  override def -(that: Nat): Nat =
    if (that.isZero)
      this
    else
      nat - that.predecessor

  override def toString: String = s"Successor($nat)"
}


object Main extends App{
  val two = new Successor(new Successor(Zero))
  val one = new Successor(Zero)
  println(two)
  println(one)
  println(two - one)
  println(two + one)
}