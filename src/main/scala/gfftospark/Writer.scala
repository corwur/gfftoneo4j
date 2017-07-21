package gfftospark

class Writer[A](val logs: Seq[String], val value: A) extends Serializable {

  def map[B](fn: A => B): Writer[B] = new Writer(logs, fn(value))

  def flatMap[B](fn: A => Writer[B]): Writer[B] = {
    val next = fn(value)
    new Writer(logs ++ next.logs, next.value)
  }

  def combine[B, C](that: Writer[B], fn: (A, B) => C): Writer[C] =
    new Writer(this.logs ++ that.logs, fn(this.value, that.value))
}

object Writer {
  def apply[A](value: A): Writer[A] = new Writer(Seq.empty, value)
  def apply[A](logs: Seq[String], value: A): Writer[A] = new Writer(logs, value)
}