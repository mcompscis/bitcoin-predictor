
package ca.uwaterloo.cs451.final_project

import fr.acinq.bitcoin.Block
import fr.acinq.bitcoin.Protocol._
import java.io.{File, DataInputStream, FileInputStream, ByteArrayInputStream, InputStream}
import scala.annotation.tailrec 
import org.rogach.scallop._
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner
import scala.collection.mutable.Map
import scala.math.exp
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import java.io._


class GetReceiverAccountsDataConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(blockpath)
  val blockpath = opt[String](descr = "block file", required = false)
  verify()
}

object GetReceiverAccountsData {
        
    def main(argv: Array[String]): Unit = {
        val args = new GetReceiverAccountsDataConf(argv)

        val conf = new SparkConf().setAppName("Receiver Accounts Data")
        val sc = new SparkContext(conf)

        val btcUsdDataPath = "Coinbase_BTCUSD.pq"
        val spark = SparkSession.builder.getOrCreate
        spark.sparkContext.setLogLevel("ERROR")
        var btc_usd_rdd = spark.read.parquet(btcUsdDataPath).rdd
                                .map(x => (x(0).toString.toLong,(x(1).toString.toDouble, x(2).toString.toDouble,
                                x(3).toString.toDouble, x(4).toString.toDouble, x(5).toString.toDouble,
                                x(6).toString, x(7).toString.toDouble))
                                )
        
                                // (unix,(low,high,open,close,volume,date,vol_fiat))
        val btcUsdRDDMapBroadcast = sc.broadcast(btc_usd_rdd.collectAsMap)

        // val rcvrAcctRawDataPath = "receiver_account_raw_data"
        val rcvrAcctRawDataPath = "train_receiver_account_raw_data"
        val rcvrAcctRDD = sc.textFile(rcvrAcctRawDataPath)
                            .map(line => {                                
                                val arr = line.split(",")
                                // (unixtime, publicKeyHash, numTransactions, totalAmtBTC, btc_usd_close)
                                (arr(0).toLong, arr(1).toString, arr(2).toInt, arr(3).toDouble, arr(4).toDouble)
                            })

        val numBlocks = rcvrAcctRDD.map(x => x._1).distinct.count
        var rcvrAcctCountByTimeStamp = rcvrAcctRDD.map(x => (x._2, 1)).reduceByKey((x, y) => x+y)
        val numRcvrAccts = rcvrAcctCountByTimeStamp.count
        println(s"NUMBER OF BLOCKS (with non-zero transactions): ${numBlocks}")
        println(s"NUMBER OF RECEIVER ACCOUNTS: ${numRcvrAccts}")
        var rcvrAcctPercentageOfBlocks = rcvrAcctCountByTimeStamp.map(x => (x._1, (1D*x._2)/numBlocks))
        val numReceiversShowUpGt20PctBlocks = rcvrAcctPercentageOfBlocks.filter(x => x._2>0.2).count
        val numReceiversShowUpGt10PctBlocks = rcvrAcctPercentageOfBlocks.filter(x => x._2>0.1).count
        val numReceiversShowUpGt5PctBlocks = rcvrAcctPercentageOfBlocks.filter(x => x._2>0.05).count
        val numReceiversShowUpGt2_5PctBlocks = rcvrAcctPercentageOfBlocks.filter(x => x._2>0.025).count
        val numReceiversShowUpGt1_25PctBlocks = rcvrAcctPercentageOfBlocks.filter(x => x._2>0.0125).count
        println(s"NUMBER OF RECEIVERS REPEATING IN >= 20% of Blocks: ${numReceiversShowUpGt20PctBlocks}")
        println(s"NUMBER OF RECEIVERS REPEATING IN >= 10% of Blocks: ${numReceiversShowUpGt10PctBlocks}")
        println(s"NUMBER OF RECEIVERS REPEATING IN >= 5% of Blocks: ${numReceiversShowUpGt5PctBlocks}")
        println(s"NUMBER OF RECEIVERS REPEATING IN >= 2.5% of Blocks: ${numReceiversShowUpGt2_5PctBlocks}")
        println(s"NUMBER OF RECEIVERS REPEATING IN >= 1.25% of Blocks: ${numReceiversShowUpGt1_25PctBlocks}")
        // Let's define Popular receiver accounts as receiver accounts showing up in >= 5% of blocks (100 blocks)
        val popularReceivers = rcvrAcctPercentageOfBlocks.filter(x => x._2>0.0075) // 0.025 means receiver shows up in at least 50 blocks
        // < 100 blocks (< 00)

        val rcvrAcctWithAddrKey = rcvrAcctRDD.map(x => (x._2, (x._1, x._3, x._4, x._5)))
        // (publicKeyHash, (unixtime, numTransactions, totalAmtBTC, btc_usd_close))

        val popularRcvrAcctData = rcvrAcctWithAddrKey.join(popularReceivers)
                                        .map(x => (x._2._1._1, x._1, x._2._1._3, x._2._1._4))
                                        // (unixtime, publicKeyHash, totalAmtBTC, btc_usd_close)
                                        .map(x => {
                                            val btcUSDPrice30MinLater = btcUsdRDDMapBroadcast.value(x._1+(30*60))._4
                                            val btcUSDPrice1HrLater = btcUsdRDDMapBroadcast.value(x._1+(1*60*60))._4
                                            val btcUSDPrice2HrLater = btcUsdRDDMapBroadcast.value(x._1+(2*60*60))._4
                                            val btcUSDPrice3HrLater = btcUsdRDDMapBroadcast.value(x._1+(3*60*60))._4
                                            val btcUSDPrice4HrLater = btcUsdRDDMapBroadcast.value(x._1+(4*60*60))._4
                                            (x._1, x._2, x._3, x._4, btcUSDPrice30MinLater, btcUSDPrice1HrLater, btcUSDPrice2HrLater, btcUSDPrice3HrLater, btcUSDPrice4HrLater)
                                        })
                                        // (unixtime, publicKeyHash, totalAmtBTC, btc_usd_close, btcUSDPrice30MinLater, btcUSDPrice1HrLater, btcUSDPrice2HrLater, btcUSDPrice3HrLater, btcUSDPrice4HrLater)
        val rcvrAcctAggregateStats = popularRcvrAcctData.map(x => (x._2, (x._1, x._3, x._4, x._5, x._6, x._7, x._8, x._9)))
                                            // (publicKeyHash, (unixtime,totalAmtBTC, btc_usd_close, btcUSDPrice30MinLater, btcUSDPrice1HrLater, btcUSDPrice2HrLater, btcUSDPrice3HrLater, btcUSDPrice4HrLater))
                                            .map(x => {
                                                val increaseInPrice30MinLater = if (x._2._4 > x._2._3) 1 else 0
                                                val increaseInPrice1HrLater = if (x._2._5 > x._2._3) 1 else 0
                                                val increaseInPrice2HrLater = if (x._2._6 > x._2._3) 1 else 0
                                                val increaseInPrice3HrLater = if (x._2._7 > x._2._3) 1 else 0
                                                val increaseInPrice4HrLater = if (x._2._8 > x._2._3) 1 else 0
                                                (x._1, (1, x._2._2, increaseInPrice30MinLater, increaseInPrice1HrLater, increaseInPrice2HrLater, increaseInPrice3HrLater, increaseInPrice4HrLater))
                                            })
                                            // (publicKeyHash, (1, totalAmtBTC, inc30MinLater, inc1HrLater, inc2HrLater, inc3HrLater, inc4HrLater))
                                            .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6, x._7 + y._7))
                                            // (publicKeyHash, (numBlocks, totalAmtBtc, numTimes30minIncrease, numTimes1HrIncrease, numTimes2HrIncrease, numTimes3HrIncrease, numTimes4HrIncrease))
                                            .map(x => (x._1, x._2._1, x._2._2, (1.0f*x._2._3)/x._2._1, (1.0f*x._2._4)/x._2._1, 
                                            (1.0f*x._2._5)/x._2._1, (1.0f*x._2._6)/x._2._1, (1.0f*x._2._7)/x._2._1))

                                        // (publickKeyHash, numBlocksTheyParticipatedIn, howManyTimesBtcPriceWentUp30MinLater, howManyTimesBtcPriceWentUp1HourLater)
                                        // (rcvr_addr, 200, 120, 180)
                                        // (rcvr_addr, 200, 120/200, 180/200)
        
            val thresholds = List(0.9, 0.8, 0.75, 0.7, 0.65)
            // thresholds.foreach(threshold => new PrintWriter(new File(s"rcvr_addresses_with_acc_${threshold}.txt")))
            // val thresholdToHighValueAddrMap = Map[Float, Array[(String, Int, Double, Float, Float, Float, Float, Float)]]()
            // val thresholdToFileMap = Map[Float, List[PrintWriter]]()
            thresholds.foreach(threshold => {
                val arr = rcvrAcctAggregateStats.sortBy(x => x._8, false).filter(x => x._8 >= threshold).collect()
                val pw = new PrintWriter(new File(s"rcvr_addresses_with_acc_${(threshold*100).toInt}.txt"))
                arr.foreach(x => pw.write(s"$x\n"))
                pw.close
            })

            
        

        
        // (unixtime, btc_usd_close, publicKeyScript, totalAmtReceivedInBlock, numOccurrencesAsReceiverInBlock)


        // val rcvrRDDWithPredictionBTCTimeStamp = rcvrTextFile.map(x => {
        //     val prediction_window_time = 1*60*60 // making prediction for one hour later, so joining with 1 hour later price
        //     val timestamp_for_prediction = x._1 + (prediction_window_time)
        //     (timestamp_for_prediction, x)
        // })



  }
}
