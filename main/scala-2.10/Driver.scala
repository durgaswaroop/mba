import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by sukmof on 16/06/2015.
 */
class Driver {


  def main(args: Array[String]): Unit ={

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0)).map(_.split(" ")).zipWithUniqueId().map(x=>(x._2,x._1))
    data.persist()
    val n  = data.count()
    val products = data.flatMap(x=>x._2).distinct().collect()
    val filtered_products = products.filter(x=>MBA.support(data,x,n)>0.007)
    val result = filtered_products.map{x: String=>
      filtered_products.map(y=> if (y!=x)
        (x,y,MBA.support(data,x,y,n),MBA.confidence(data,x,y),MBA.lift(data,x,y,n))
      else
        null)
    }
    val persist = sc.parallelize(result)
    persist.map(x=>x.filter(_!=null)).flatMap(x=>x).saveAsTextFile(args(1))

  }
}
