import java.util.Random

import scala.util.control.Breaks

object Lottery {
  def result(x :Int) :Unit = x match {
    case 0 => println("很遗憾您没有中奖")
    case 1 => println("恭喜您中了七等奖")
    case 2 => println("恭喜您中了六等奖")
    case 3 => println("恭喜您中了五等奖")
    case 4 => println("恭喜您中了四等奖")
    case 5 => println("恭喜您中了三等奖")
    case 6 => println("恭喜您中了二等奖")
    case 7 => println("恭喜您中了一等奖")
  }
  def choseNum(): Array[Int] = {
    val lottery = new Array[Int](7)
    var i = 0
    val loop = new Breaks
    while(i < lottery.length) {
      var buyNum = readInt()
       if(buyNum > 0 && buyNum < 36){
         lottery(i) = buyNum
         loop.breakable{
           for(j <- 0 to i-1){
             if(lottery(i) == lottery(j)){
               println("输入数据重复请重新输入")
               i-=1
               loop.break()
             }
           }
         }
         i+=1
       }
      else{
         println("请输入规定范围内的数字")
         i-=1
       }
    }
    lottery
  }
  def ranNum(): Array[Int] = {
    var result = new Array[Int](7)
    for(i <- 0 until result.length){
      val randomNum = (new Random).nextInt(36)+1
      if(!result.exists(num => num == randomNum))
        result(i) = randomNum
    }
    result.foreach(println)
    result
  }
  def matchCount(chose: Array[Int],result: Array[Int]) : Int = {
    var count = 0
    for(i <- 0 until chose.length){
        if(result.contains(chose(i)))
          count+=1
    }
    count
  }
  def main(args: Array[String]): Unit = {

    println("欢迎使用彩票系统")
    println("请输入您选择的号码1-35之间")
    var cho = choseNum()
    var ran = ranNum()
    result(matchCount(cho,ran))
  }

}
