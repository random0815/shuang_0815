package function

object Money {
  class Travse(val num : Double){
      def +%(per : Double) : Double = num + num*per*0.01
  }

  def main(args: Array[String]): Unit = {
    implicit def test(n : Double) = new Travse(n)
    println(15+%15)
  }



}
