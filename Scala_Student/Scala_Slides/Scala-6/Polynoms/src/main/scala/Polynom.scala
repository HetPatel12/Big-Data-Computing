class Polynom(nonZeroTerms: Map[Int, Double]) {
  def this(bindings: (Int, Double)*) = this(bindings.toMap)

  def terms: Map[Int, Double] = nonZeroTerms.withDefaultValue(0.0)

  def + (other: Polynom): Polynom = {
    def addTerm(terms: Map[Int, Double], term: (Int, Double)): Map[Int, Double] =
      terms + (term._1 -> (term._2 + terms(term._1)))

    new Polynom(other.terms.foldLeft(terms)(addTerm))
  //  slower: new Polynom(terms ++ other.terms.map(pair => (pair._1, terms(pair._1) + pair._2)))
  }

  override def toString: String ={
    val termStrings = for ( (exponent, coefficient) <- terms.toList.sorted.reverse )
      yield if (coefficient == 0) "" else s"($coefficient * x ^ $exponent)"
    if (termStrings.isEmpty)
      "0"
    else
      termStrings.mkString(" + ")
  }
}


object Main extends App{
  val x = new Polynom(0 -> 2, 1 -> -3, 2 -> 1)
  println(x)
  println(x + x)
}