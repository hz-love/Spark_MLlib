package spark_mllib.lession1

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Author :  hz.love
  * Time :  2018/3/15
  * description:
  * :
  */
object rdd_test01 {
	def main(args: Array[String]): Unit = {
		val session: SparkSession = SparkSession.builder().master("local[2]").appName("rdd_test01").getOrCreate()
		val sc: SparkContext = session.sparkContext

		// 随机抽取
		val a = sc.parallelize(1 to 1000, 2)
		val l: Long = a.sample(false, 0.2, 0).count()
		println(s"sample's result: $l")

		// 取交集
		val rdd1: RDD[Int] = sc.parallelize(1 to 9, 3)
		val rdd2: RDD[Int] = sc.parallelize(5 to 15, 3)
		rdd1.intersection(rdd2).collect.foreach(println)

		// 聚合
		//z.aggregate(0)(math.max(_, _),  _ + _)
		val z = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3)))
		z.aggregateByKey(0)(math.max, _ + _).collect.foreach(println)

		// 随机划分
		val rdd5: Array[RDD[Int]] = a.randomSplit(Array(0.3, 0.7), 1)
		val l1: Long = rdd5(0).count()
		val l2: Long = rdd5(1).count()
		println(s"randomSplit's result: $l1, $l2")

	}
}
