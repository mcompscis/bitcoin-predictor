
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


/*
A Script to get raw transaction level data and block-receiver acct level data

*/
class GetRawDataConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(blockpath)
  val blockpath = opt[String](descr = "block file", required = false)
  verify()
}

object GetRawData {

    def readBlock(input: DataInputStream): Block = {
        val magic = uint32(input)
        assert(magic == 0xd9b4bef9L)
        val size = uint32(input)
        val raw = new Array[Byte](size.toInt)
        input.readFully(raw)
        val block = Block.read(new ByteArrayInputStream(raw))
        block
    }

    def determinePubKeyHash(input: String): String = {
            val inputLen = input.length()

            //paytopubhashkey
            if (input.startsWith("76a914") && input.endsWith("88ac")) {
                input.substring("76a914".length, input.length - "88ac".length)
            }
            //paytoscripthash
            else if (input.startsWith("a914") && input.endsWith("87")) {
                input.substring("a914".length, input.length - "87".length)
            }
            //paytopubkey
            else if (input.endsWith("ac")) {
                val pub = input.substring(0, input.length - "ac".length)
                Crypto.hash160(ByteVector.fromHex(pub).get).toHex
            }
            else {
                input
            }
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
        val args = new GetRawDataConf(argv)

        val conf = new SparkConf().setAppName("Raw Data Bitcoin Parser")
        val sc = new SparkContext(conf)

        val rawTransactionsOutputDir = "raw_transactions"
        val rcvrAcctRawDataOutputDir = "receiver_account_raw_data"
        val trainRcvrAcctDataOutputDir = "train_receiver_account_raw_data"
        val testRcvrAcctDataOutputDir = "test_receiver_account_raw_data"

        val outputDirs = List(rawTransactionsOutputDir, rcvrAcctRawDataOutputDir, trainRcvrAcctDataOutputDir, testRcvrAcctDataOutputDir)
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
        
        var txns = blocks.flatMap(block => {
            var buffer = ArrayBuffer[(Long, String, Int, Int, Double, String)]()
            val blockFloorTime = (block.header.time / 60)*60 // blockFloorTime is block timestamp truncated to minute-level granularity
            val txns = block.tx.drop(1) // dropping the coinbase transaction (miner's reward)
            txns.foreach(tx => {
                val txid = tx.txid.toHex
                val numReceivers = tx.txOut.length
                val numUniqueReceivers = tx.txOut.map(txOut => txOut.publicKeyScript.toHex).toSet.size
                tx.txOut.foreach(txOut => {
                    val amountInBtc = txOut.amount.toBtc.toDouble
                    val publicKeyScript = txOut.publicKeyScript.toHex
                    val publicKeyHash = determinePubKeyHash(publicKeyScript)
                    buffer += ((blockFloorTime, txid, numReceivers, numUniqueReceivers, amountInBtc, publicKeyHash))
                })
            })
            buffer
        })
        // (unixtime, txid, numReceivers, numUniqueReceivers, amountInBtc, publicKeyScript)

        // println("MINIMUM TRANSACTION TIME: " + txns.takeOrdered(1)(Ordering[Long].on(x => x._1))(0))
        // println("MAXIMUM TRANSACTION TIME: " + txns.top(1)(Ordering[Long].on(x => x._1))(0))

        val blockWithRcvrAcctData = txns.map(txn => ((txn._1, txn._6), (txn._5, 1))) // ((unixtime, publicKeyScript), (amt, 1))
                                    .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
                                    .map(txn => (txn._1._1, (txn._1._2, txn._2._1, txn._2._2)))

        val blockWithRcvrAcctBtcUsdData = blockWithRcvrAcctData.map(x => (x._1, btcUsdRDDMapBroadcast.value(x._1)._4, x._2._1, x._2._2, x._2._3))
        // (unixtime, btc_usd_price, public_key_script, amtInBTCReceivedByThatAddressInThatBlock, numTransactions they received in block)
        val blockTimestampsRDD = blockWithRcvrAcctData.map(x => x._1).distinct.sortBy(x => x, true).zipWithIndex()
        val finalTrainingPointUnixTime = blockTimestampsRDD.filter(x => (x._2 == 1999)).take(1)(0)._1  // NOTE: Hardcoding first 2000 points as training
        val numBlocks = blockTimestampsRDD.count
        val trainData = blockWithRcvrAcctBtcUsdData.filter(x => (x._1 <= finalTrainingPointUnixTime))
        val testData = blockWithRcvrAcctBtcUsdData.filter(x => (x._1 > finalTrainingPointUnixTime))
        // (unixtime, btc_usd_close, publicKeyScript, totalAmtReceivedInBlock, numOccurrencesAsReceiverInBlock)
        val trainDataTextFile = trainData.map(x => s"${x._1},${x._3},${x._5},${x._4},${x._2}")
        val testDataTextFile = testData.map(x => s"${x._1},${x._3},${x._5},${x._4},${x._2}")
        val textOutputRDD = blockWithRcvrAcctBtcUsdData.map(x => s"${x._1},${x._3},${x._5},${x._4},${x._2}")
        // (unixtime, publicKeyHash, numTransactions, totalAmtBTC, btc_usd_close)
        textOutputRDD.saveAsTextFile(rcvrAcctRawDataOutputDir)
        trainDataTextFile.saveAsTextFile(trainRcvrAcctDataOutputDir)
        testDataTextFile.saveAsTextFile(testRcvrAcctDataOutputDir)


        // TODO: Next step, look into receiver addresses that appear in multiple timestamps



        // txns.saveAsTextFile(rawTransactionsOutputDir)

  }
}
