trait A {
  def a: String
}

trait AA {
  def aa: String = "aa"
}

trait B {
  this: A =>
  def b: String = "b"
}

object Test extends App {

  def printa(a: A): Unit = {
    println(a.a)
  }

  lazy val a = {
    println(1)
  }

  val some = new B with A {
    override val a = "a"
    println(a)
  }
  printa(some)
  //  println(some.a)
}
