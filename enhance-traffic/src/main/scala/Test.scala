class Stack[+A] {
  def push[B >: A](elem: B): Stack[B] = new Stack[B] {
    override def top: B = elem

    override def pop: Stack[B] = Stack.this

    override def toString() = elem.toString() + " " +
        Stack.this.toString()
  }

  def top: A = sys.error("no element on stack")

  def pop: Stack[A] = sys.error("no element on stack")

  override def toString() = ""
}

object VariancesTest extends App {
  var s: Stack[String] = new Stack().push("hello")
  var t: Stack[Any] = s.push(3)
  println(s)
}