import scala.collection.mutable.ArrayBuffer

abstract class Queue {
  println("queue")
  def put(x : Int)
  def get() : Int
}
class QueueInt extends Queue{
  println("queueint")
  val array = new ArrayBuffer[Int]
  override def put(x: Int): Unit = { array += x}
  override def get(): Int = array.remove(0)
}
trait Doubling extends Queue{

 abstract override def put(x: Int): Unit ={
    super.put(2 * x)
  }

}
trait Incrementing extends Queue{
  abstract override def put(x: Int): Unit = {
    super.put(x + 1)
  }
}
trait Filtering extends Queue{
  abstract override def put(x: Int): Unit = {
    if(x >= 0)
    super.put(x)
  }

}
object QueueTest{
  def main(args: Array[String]): Unit = {
    val q1 = new QueueInt with Incrementing
    val q2 = new QueueInt with Doubling
    val q3 = new QueueInt with Filtering
    q1.put(5)
    q2.put(9)
    q3.put(-1)
    println(q1.get())
    println(q2.get())
   // println(q3.get())

  }
}