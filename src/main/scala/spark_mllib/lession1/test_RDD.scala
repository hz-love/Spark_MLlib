package spark_mllib.lession1

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Author :  hz.love
  * Time :  2018/3/15 
  * description:
  * :	combineByKey
  * 		combineByKey[C](createCombiner:(V)=>C, mergeValue: (C,V)=>C,
  * 					mergeCombiners: (C,C)=>C, numPartitions: Int):RDD[(K,C)]
  */
object test_RDD {
	def main(args: Array[String]): Unit = {
		val session: SparkSession = SparkSession.builder().appName("test_RDD").master("local[2]").getOrCreate()
		val sc: SparkContext = session.sparkContext

		val data = Array((1,1.0), (1,2.0), (1,3.0), (2,4.0), (2,5.0), (2,6.0))
		val rdd: RDD[(Int, Double)] = sc.parallelize(data, 2)

		val collect: Array[(Int, (Double, Int))] = rdd.combineByKey(createCombiner = (v: Double) => (v: Double, 1),
			mergeValue = (c: (Double, Int), v: Double) => (c._1 + v, c._2 + 1),
			mergeCombiners = (c1: (Double, Int), c2: (Double, Int)) => (c1._1 + c2._1, c1._2 + c2._2),
			numPartitions = 2).collect

		collect.foreach(println)
	}
}
