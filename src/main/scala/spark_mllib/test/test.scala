package spark_mllib.test

import scala.collection.immutable

/**
  * Author :  hz.love
  * Time :  2018/3/15 
  * description:
  * :
  */
object test {
	def main(args: Array[String]): Unit = {
		val l1 = List(1,2,3,4,5)
		val l2 = List(3,4,5,6,7)

		val ints = l1 diff l2
		println(ints)
	}
}
