
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

        val conf = new SparkConf().setAppName("Bitcoin Block Parser")
        val sc = new SparkContext(conf)

        val rawTransactionsOutputDir = "raw_transactions"
        val rcvrAcctRawDataOutputDir = "receiver_account_raw_data"

        val outputDirs = List(rawTransactionsOutputDir, rcvrAcctRawDataOutputDir)
        outputDirs.foreach(outputDir => {
            FileSystem.get(sc.hadoopConfiguration).delete(new Path(outputDir), true)
        })

        val sparkSession = SparkSession.builder.getOrCreate
        var btc_usd_rdd = sparkSession.read.parquet("Coinbase_BTCUSD.pq").rdd
                                .map(x => (x(0).toString.toLong,(x(1).toString.toDouble, x(2).toString.toDouble,
                                x(3).toString.toDouble, x(4).toString.toDouble, x(5).toString.toDouble,
                                x(6).toString, x(7).toString.toDouble))
                                )
                                // (unix,(low,high,open,close,volume,date,vol_fiat))
        
                
        var binaryFiles = sc.binaryFiles("bitcoin_blocks")
        // binaryFiles = sc.parallelize(binaryFiles.take(1))
        var blocks = binaryFiles.flatMap(binaryFile => {
            var blocks = readBlocks(binaryFile._2.open(), List())
            blocks
        })
        
        var txns = blocks.flatMap(block => {
            var buffer = ArrayBuffer[(Long, String, Int, Int, Double, String)]() // TODO: Add parameter signature
            val blockFloorTime = (block.header.time / 300)*300 // block time floored to some 5-minute interval to join with BTC_USD price
            val txns = block.tx.drop(1) // dropping the coinbase transaction (miner's reward)
            txns.foreach(tx => {
                val txid = tx.txid.toHex
                val numReceivers = tx.txOut.length
                val numUniqueReceivers = tx.txOut.map(txOut => txOut.publicKeyScript.toHex).toSet.size
                tx.txOut.foreach(txOut => {
                    val amountInBtc = txOut.amount.toBtc.toDouble
                    val publicKeyScript = txOut.publicKeyScript.toHex
                    buffer += ((blockFloorTime, txid, numReceivers, numUniqueReceivers, amountInBtc, publicKeyScript))
                    // TODO: Add logic for PublicKeyScript to Public key hash conversion when possible
                })
            })
            buffer
        })

        // blocks.map(x => (x.header.time/300)*300)
        // (unixtime, txid, numReceivers, numUniqueReceivers, amountInBtc, publicKeyScript)

        println("MINIMUM TRANSACTION TIME: " + txns.takeOrdered(1)(Ordering[Long].on(x => x._1))(0))
        println("MAXIMUM TRANSACTION TIME: " + txns.top(1)(Ordering[Long].on(x => x._1))(0))

        val blockWithRcvrAcctData = txns.map(txn => ((txn._1, txn._6), (txn._5, 1))) // ((unixtime, publicKeyScript), (amt, 1))
                                    .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
                                    .map(txn => (txn._1._1, (txn._1._2, txn._2._1, txn._2._2)))

        val blockWithRcvrAcctBtcUsdData = btc_usd_rdd.join(blockWithRcvrAcctData).map(x => (x._1, x._2._1._4, x._2._2._1, x._2._2._2, x._2._2._3))
        // (unixtime, btc_usd_close, publicKeyScript, totalAmtReceivedInBlock, numOccurrencesAsReceiverInBlock)
        // TODO: Get 15min or 1 hour later data as well and join?

        blockWithRcvrAcctBtcUsdData.saveAsTextFile(rcvrAcctRawDataOutputDir)
        // NOTE: 2 blocks within same 5-minute interval counted as one block 
        // ((unixtime, publicKeyScript), (totalAmountBtc))
        // txns.map(txn => ((txn._1, (txn._2, txn._3, txn._4, txn._5, txn._6)))
        // .
         // key is the unixtime



        txns.saveAsTextFile(rawTransactionsOutputDir)

  }
}
