package spark.examples

import scala.math.random
import spark._
import SparkContext._

object SparkLn2 {
    def main(args: Array[String]) = {
        if (args.length == 0) {
            sys.error("Usage: SparkLn2 <host> [<slices>]")
            sys.exit(0)
        }

        val host: String = args(0)
        val slices: Int = if (args.length >= 2) args(1).toInt else 4
        val n = 10000000 * slices

        val spark = new SparkContext(host, "SparkLn2")
        val count = spark.parallelize(0 until n, slices).map { case _ =>
            val x = random + 1.0
            val y = random
            if (y <= 1 / x.toDouble) 1 else 0
        }.reduce(_ + _)
        println(s"ln2 in roughly ${count / n.toDouble}")
    }
}