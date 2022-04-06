
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
import fr.acinq.bitcoin.Crypto
import scodec.bits._
import java.io._


/*
A Script to get raw transaction level data and block-receiver acct level data

*/
class GetBlockActivityFeaturesDataConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(blockpath)
  val blockpath = opt[String](descr = "block file", required = false)
  verify()
}

object GetBlockActivityFeaturesData {

    def readBlock(input: DataInputStream): Block = {
        val magic = uint32(input)
        assert(magic == 0xd9b4bef9L)
        val size = uint32(input)
        val raw = new Array[Byte](size.toInt)
        input.readFully(raw)
        val block = Block.read(new ByteArrayInputStream(raw))
        block
    }

    def readBlocks(input: DataInputStream, blocks: List[Block]): List[Block] = {
        if (input.available() <= 0) {
            blocks
        } else {
            val block = readBlock(input)
            readBlocks(input, block :: blocks)
        }
    }
        
    def main(argv: Array[String]): Unit = {
        val args = new GetBlockActivityFeaturesDataConf(argv)

        val conf = new SparkConf().setAppName("Bitcoin Block Parser")
        val sc = new SparkContext(conf)

        val rawTransactionsOutputDir = "raw_transactions"
        val outputDir = "NumTransactionsPerBlock"

        val outputDirs = List(outputDir)
        outputDirs.foreach(outputDir => {
            FileSystem.get(sc.hadoopConfiguration).delete(new Path(outputDir), true)
        })

        val btcUsdDataPath = "Coinbase_BTCUSD.pq"
        val sparkSession = SparkSession.builder.getOrCreate
        var btc_usd_rdd = sparkSession.read.parquet(btcUsdDataPath).rdd
                                .map(x => (x(0).toString.toLong,(x(1).toString.toDouble, x(2).toString.toDouble,
                                x(3).toString.toDouble, x(4).toString.toDouble, x(5).toString.toDouble,
                                x(6).toString, x(7).toString.toDouble))
                                )
                                // (unix,(low,high,open,close,volume,date,vol_fiat))
        
        val btcUsdRDDMapBroadcast = sc.broadcast(btc_usd_rdd.collectAsMap)

        var binaryFiles = sc.binaryFiles("bitcoin_blocks")
        // binaryFiles = sc.parallelize(binaryFiles.take(1))
        var blocks = binaryFiles.flatMap(binaryFile => {
            var blocks = readBlocks(binaryFile._2.open(), List())
            blocks
        })
        
        var numTransactionsPerBlock = blocks.map(block => {
            val blockFloorTime = (block.header.time / 60)*60 // blockFloorTime is block timestamp truncated to minute-level granularity
            // val txns = block.tx.drop(1) // dropping the coinbase transaction (miner's reward)
            val txns = block.tx
            val numTransactions = txns.length
            val coinbaseTxn = txns(0)
            val totalMinerReward = coinbaseTxn.txOut(0).amount.toBtc.toDouble // miner's reward + fee reward
            val feeReward = totalMinerReward - 6.25
            val totalBtcAmountReceivedInBlock = block.tx.map(txn => txn.txOut.map(txOut => txOut.amount.toBtc.toDouble).sum).sum
            (blockFloorTime, (txns.length, totalBtcAmountReceivedInBlock, feeReward))
        }).reduceByKey((x, y) => (x._1+y._1, x._2+y._2, x._3+y._3))

        var finalRDD = numTransactionsPerBlock.map(x => (x._1, x._2._1, x._2._2, x._2._3, 
        btcUsdRDDMapBroadcast.value(x._1)._4, btcUsdRDDMapBroadcast.value(x._1+(4*60*60))._4,
        btcUsdRDDMapBroadcast.value(x._1+(6*60*60))._4, btcUsdRDDMapBroadcast.value(x._1+(12*60*60))._4, btcUsdRDDMapBroadcast.value(x._1+(24*60*60))._4))
                                    .sortBy(x => x._1, true)
                                    .map(x => s"${x._1},${x._2},${x._3},${x._4},${x._5},${x._6},${x._7},${x._8},${x._9}")
        val finalData = finalRDD.collect()
        val pw = new PrintWriter("numTransactionsPerBlock.csv")
        pw.write("block_timestamp,num_txns_in_block,total_btc_in_block,total_fee_reward,btc_usd_price_at_timestamp,btc_usd_price_4_hrs_later,btc_usd_price_6_hrs_later,btc_usd_price_12_hrs_later,btc_usd_price_24_hrs_later\n")
        finalData.foreach(x => pw.write(s"$x\n"))
        pw.close
        // finalRDD.coalesce(1).saveAsTextFile(outputDir)

  }
}
