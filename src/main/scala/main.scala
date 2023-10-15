import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
object main {
  case class MatchedOrder(
                           BuyOrderId: String,
                           BuyPrice: Long,
                           BuyQuantity: Long,
                           SellOrderId: String,
                           SellPrice: Long,
                           SellQuantity: Long,
                           MatchTime: String
                         )
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf()
      .setAppName("dheeraj-spark-demo")
      .setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()
    val orders=spark.read.csv("file:///Users/tarunreddy/Downloads/coding_excercise/exampleOrders.csv").toDF("OrderId","UserName","OrderTime","OrderType","Quantity","Price")
    val bestbutpricedf=udf{
      (price:Long)=> price
    }

    val sellbpricedf = udf {
      (price: Long) => -price
    }
    val buyorders = orders.filter(col("OrderType") === "BUY")
      .withColumn("BestPrice", bestbutpricedf(col("Price")))

    val sellorders = orders.filter(col("OrderType") === "SELL")
      .withColumn("BestPrice", sellbpricedf(col("Price")))

    // Create an order book for unmatched orders
    var orderBook = Seq.empty[DataFrame]

    // Create an empty DataFrame for matched orders
    val matchedOrdersSchema: StructType = StructType(
      Seq(
        StructField("BuyOrderId", StringType, nullable = false),
        StructField("BuyPrice", LongType, nullable = false),
        StructField("BuyQuantity", LongType, nullable = false),
        StructField("SellOrderId", StringType, nullable = false),
        StructField("SellPrice", LongType, nullable = false),
        StructField("SellQuantity", LongType, nullable = false),
        StructField("MatchTime", StringType, nullable = false)
      )
    )
    var matchedOrdersDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], matchedOrdersSchema)
      val sortedBuyOrders = buyorders.sort("BestPrice", "OrderTime")
      val sortedSellOrders = sellorders.sort("BestPrice", "OrderTime")

      // Match orders with the same quantity
      val matched = sortedBuyOrders.join(sortedSellOrders, Seq("Quantity"), "inner")
        .select(sortedBuyOrders("OrderId").as("BuyOrderId"),
                sortedBuyOrders("Price").as("BuyPrice"),
          sortedBuyOrders("Quantity").as("buyQuantity"),
          sortedSellOrders ("OrderId").as("SellOrderId"),
          sortedSellOrders("Price").as("SellPrice"),
          sortedSellOrders("Quantity").as("SellQuantity"),
          sortedBuyOrders("OrderTime").as("MatchTime")
        )

      // If there are matches, record the matched orders
      if (matched.count() > 0) {
        matchedOrdersDF = matchedOrdersDF.union(matched)
        // Remove matched orders from buy and sell DataFrames
        val remainingBuyOrders = buyorders.except(matched)
        val remainingSellOrders = sellorders.except(matched)

        // Update the order book with remaining unmatched orders
        orderBook = orderBook :+ remainingBuyOrders :+ remainingSellOrders
      } else {
        // No matches found, add both buy and sell orders to the order book
        orderBook = orderBook :+ buyorders :+ sellorders

      }

  }
}


