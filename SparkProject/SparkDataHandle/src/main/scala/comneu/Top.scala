package comneu

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shuangmm on 2017/11/28
  */
object Top {
  def main(args: Array[String]): Unit = {
    val conf =new SparkConf().setAppName("read_gz_file").setMaster("local")
    val sc =new SparkContext(conf)
    val user = sc.textFile("users.dat")
    val movies = sc.textFile("movies.dat")
    val rating = sc.textFile("ratings.dat")
    val M_user = user.map(_.split("::")).filter(temp =>
      temp(2).toInt >= 18 & temp(2).toInt <= 24 & temp(1).equals("M") ).map{
      x =>(x(0),(x(1),x(2)))
    }
    val M_user1 = user.map(_.split("::")).filter(temp => temp(1).equals("M") ).map{
      x =>(x(0),(x(1),x(2)))
    }
    val F_user =  user.map(_.split("::")).filter(temp => temp(1).equals("F") ).map{
      x =>(x(0),(x(1),x(2)))
    }
    val mov_Id = movies.map(_.split("::")).map(x =>(x(0),x(1)))
    val usr_movie = rating.map(_.split("::")).map(x => (x(0),x(1)))
    val usrMo_Id = M_user.join(usr_movie).map(x => (x._1,x._2._2))//ɸѡ�������û���������Ӧ�ĵ�Ӱ���
    val fav_movie = usrMo_Id.values.map(x =>(x,1)).reduceByKey(_+_).join(mov_Id).values.map(x=>
      (x._2,x._1)).collectAsMap()//��ϲ���ĵ�Ӱ��Ŷ�Ӧ������
    val topFav = fav_movie.toSeq.sortWith(_._2 > _._2).take(10)//��������������18-24������
    val top_rating = rating.map(_.split("::")).map(x => (x(1),x(2).toDouble)).combineByKey(
      rate => (rate,1),
      (acc:(Double,Int),rate) => (acc._1 + rate,acc._2+1),
      (acc1:(Double,Int),acc2:(Double,Int)) => (acc1._1 + acc2._1,acc1._2 + acc2._2)
    ).filter(_._2._2 > 5).map{ case (key, value) => (key, value._1 / value._2.toFloat)}

    val top_mov_name = mov_Id.join(top_rating).values.map{
      x =>(x._1,x._2)
    }.sortBy(_._2,false).take(10)
    val top_user = usr_movie.keys.map(x => (x,1)).reduceByKey(_+_).sortBy(_._2,false).take(10)
    val Man_Id = M_user1.join(usr_movie).map(x => (x._1,x._2._2))
    val top_M_fav  = Man_Id.values.map(x =>(x,1)).reduceByKey(_+_).join(mov_Id).values.map(x=>
      (x._2,x._1)).sortBy(_._2,false).take(10)
    val fe_Id = F_user.join(usr_movie).map(x => (x._1,x._2._2))
    val top_F_fav = fe_Id.values.map(x =>(x,1)).reduceByKey(_+_).join(mov_Id).values.map(x=>
      (x._2,x._1)).sortBy(_._2,false).take(10)
    top_mov_name.foreach(println)

  }

}
