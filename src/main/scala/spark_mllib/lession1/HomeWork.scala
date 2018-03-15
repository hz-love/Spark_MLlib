package spark_mllib.lession1

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Author :  hz.love
  * Time :  2018/3/15 
  * description:
  * :				上报日期			用户ID 				安装包名
  * 	数据格式： 2016-03-29		110d2230b4567fdf	com.lyrebirdstudio.mirror
  *
  * 相关名词：
  * 	关联性强度如何，由3个概念，即支持度、置信度、 提升度来控制和评价
  * 支持度：
  * 	支持度是指在所有项集中{X,Y}出现的可能性， 即项集中同时含有X和Y的概率。
  * 	通过设定最小阈值，剔除“出镜率”较低的无意义规则，保留出现较为频繁的项集所隐含的规则
  * 	举例：
  * 		设定最小阈值为5%，由于有{尿布，啤酒}的支持度800/10000=8%，满足最小阈值要求，保留
  * 		而{尿布，面包}的支持度100/10000=1%,低于最小阈值，被剔除
  * 置信度：
  * 	置信度表示在先决条件X发生的条件下，关联结果Y发生的概率：设定置信度最小阈值来进一步筛选。
  * 	举例：
  * 		设定置信度最小阈值为 70%，在{尿布，啤酒}中，购买尿布时会购买啤酒的置信度为800/1000 = 80%
  * 		保留。而购买啤酒时会购买尿布的置信度为800/2000=40%，被剔除
  */
object HomeWork {
	def main(args: Array[String]): Unit = {
		val session: SparkSession = SparkSession.builder().master("local[2]").appName("homework1").getOrCreate()
		val sc: SparkContext = session.sparkContext

		val data: RDD[Array[String]] = sc.textFile("F:\\BaiduYunDownload\\Spark Mllib机器学习算法源码及实战详解\\Spark MLlib机器学习01\\用户安装列表数据\\*.gz")
			.map(_.split("\t"))

		val datas: RDD[Array[String]] = data.cache()

		// 问题一： 通过Spark读取安装列表数据，并且统计数据总行数、用户数量、日期有哪几天；
		val totalNum: Long = datas.count()
		println(s"The total amount of data is: $totalNum")
		val totalUser: Long = datas.map(x => x(1)).distinct().count()
		println(s"The total amount of user is: $totalUser")
		println("What are the dates:")
		datas.map(x => x(1)).distinct().collect().foreach(println)

		// 问题二： 指定任意连续的2天，计算每个用户第2天的新安装包名；

		val date1 = ""
		val date2 = ""
		val tuple1: RDD[(String,User)] = datas.filter(x => x(0) == date1 || x(0) == date2)
			.map { x =>
				var user: (String, User) = null
				if (x(0) == date1) {
					user = (x(1), User(List(x(2)), List()))
				} else if (x(1) == date1) {
					user = (x(1), User(List(), List(x(2))))
				} else
					println("erro: 时间过滤不完整")
				user
			}
		val tuple2: RDD[(String, User)] = tuple1.reduceByKey(func = (x ,y) => User(x.first ++ y.first, x.seccend ++ y.seccend), 2)

		println(s"$date1 and $date2 增加了新的安装包为：")
		tuple2.foreachPartition{ x=>
			x.toArray.foreach{ f =>
				println(s"${f._1}  ====>")
				val list: List[String] = f._2.seccend diff f._2.first
				list.foreach(println)
			}
		}



		// 指定任意1天的数据，根据每个用户的安装列表，统计每个包名的安装用户数量，由大到小排序，并且取前1000个包名，
		val date3 =""
		val tuples: Array[(Int, String)] = datas.filter(x => x(0) == date3)
			.map(x => (x(1), x(2)))
			.groupByKey(2)
			.flatMap { x =>
				x._2.toArray.distinct.map(f => (f, 1))
			}
			.reduceByKey(_ + _, 2)
			.map(x => (x._2, x._1))
			.sortByKey(ascending = false, 2)
			.take(1000)
		tuples.foreach(println)

		// 最后计算这1000个包名之间的支持度和置信度，其中设置支持度的阈值是0.1%，置信度阈值30%
		// 结果需要是：包名A	包名B	支持度值   A->B的置信度	B->A的置信度


	}
}

case class User(var first: List[String], var seccend: List[String])