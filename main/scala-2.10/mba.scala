/**
 * Created by sukmof on 07/06/2015.
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}


object MBA {


 /*calculate support for itemsets for a two way association analysis*/
  def support(rdd: RDD[(Long,Array[String])],prodA: String,prodB: String,n:Long): Float= {
      val ruleCount = rdd.filter(x=>x._2.contains(prodA) && x._2.contains(prodB)).count()
      val totalCount = n
      ruleCount/totalCount.toFloat
  }

  /*calculate support for a single item*/
  def support(rdd: RDD[(Long,Array[String])],prod: String,n:Long): Float= {
    val ruleCount = rdd.filter(x=>x._2.contains(prod)).count().toFloat
    ruleCount/n.toFloat
  }

  /*calculate confidence measure for a two way association rule*/
  def confidence(rdd: RDD[(Long,Array[String])],prodA: String,prodB: String): Float= {
    val ruleCount = rdd.filter(x=>x._2.contains(prodA) && x._2.contains(prodB)).count()
    val prodACount = rdd.filter(x=>x._2.contains(prodA)).count()
    ruleCount/prodACount.toFloat
  }

  /*calculate lift measure for a two way association rule*/
  def lift(rdd: RDD[(Long,Array[String])],prodA: String,prodB: String,n:Long): Float={
    support(rdd,prodA,prodB,n)/(support(rdd,prodA,n)*support(rdd,prodB,n))
  }


}
