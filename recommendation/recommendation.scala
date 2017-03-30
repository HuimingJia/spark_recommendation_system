import java.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.jblas.DoubleMatrix
import scala.collection.immutable.SortedMap
import scala.io.Source

val inputpath="/user/hadoop16/pre-output/output.txt"
val itempath="items-medium.txt"
val indexpath="/user/hadoop16/pre-output/productindex.txt"
val outputpath="final_output.txt"

var IdToIndex = SortedMap[String,String]()
var IndexToId = SortedMap[String,String]()
val s = Source.fromFile(indexpath,"UTF-8").getLines.foreach{line =>
  val kvpair = line.split(":")
  IdToIndex += kvpair(0).trim() -> kvpair(1)
  IndexToId += kvpair(1) -> kvpair(0)
}

val listOfLines = Source.fromFile(itempath).getLines.toList
val data = sc.textFile(inputpath)
val ratings = data.map(_.split(":") match { case Array(user, item, rate) => Rating(user.toInt, item.toInt, rate.toDouble)}).cache()
//get matrix for training
val users = ratings.map(_.user).distinct()
//get userlist
val products = ratings.map(_.product).distinct()
//get productlist

val rank = 12
//the order of matrix
val lambda = 0.01
//the initial value for maxtrixfactorization
val numIterations = 20
//how many time to iterate
val model = ALS.train(ratings, rank, numIterations, 0.01)

//get result for each item in the itemlist
val writer = new PrintWriter(new File(outputpath))
for (i <- 0 until listOfLines.length) {
  val itemId = IdToIndex.get(listOfLines(i)).get.toInt
  val itemFactor = model.productFeatures.lookup(itemId).head
  val itemVector = new DoubleMatrix(itemFactor)
  val sims = model.productFeatures.map { case (id, factor) =>
    val factorVector = new DoubleMatrix(factor)
    val sim = factorVector.dot(itemVector) / (factorVector.norm2() * itemVector.norm2())
    (id, sim)
  }

  val sortedSims = sims.top(11)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
  for (n <- 0 until sortedSims.length-1){
    writer.print(IndexToId.get(sortedSims(n)._1.toString).get+",")
  }
  writer.print(IndexToId.get(sortedSims(sortedSims.length-1)._1.toString).get)
  writer.print("\n")
}
writer.close()