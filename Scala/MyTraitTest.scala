trait transform{
  def tran = println("I can transport goods")

}
abstract class Animal1{
  def sound
  def eat
}
abstract class Vehicle{
  def name
  def way


}
class hourse extends Animal1 with transform {

  override def sound = println("lalalalalala")

  override def eat: Unit = println("I eat grass")
}
class car extends Vehicle with transform {
  override def name = println("I am a car")

  override def way = println("I run on the road")
}
object MyTraitTest {
  def main(args: Array[String]): Unit = {
    val hou = new hourse
    hou.tran
    val car = new car
    car.tran


  }

}
