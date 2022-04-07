
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
import scala.collection.mutable.ArrayBuffer
import fr.acinq.bitcoin.Crypto
import scodec.bits._
import scala.io.Source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
// import spark.implicits._

/*
A Script to get raw transaction level data and block-receiver acct level data

*/
class ExtractTrainTestDataV1Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(blockpath)
  val blockpath = opt[String](descr = "block file", required = false)
  verify()
}

object ExtractTrainTestDataV1 {

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
        val args = new ExtractTrainTestDataV1Conf(argv)

        val conf = new SparkConf().setAppName("Raw Data Bitcoin Parser")
        val sc = new SparkContext(conf)

        val trainDataPath = "train_bitcoin_blocks_data"
        val validDataPath = "valid_bitcoin_blocks_data"
        val testDataPath = "test_bitcoin_blocks_data"
        val allDataPath = "test.csv"
        val outputDirs = List(allDataPath, trainDataPath, validDataPath, testDataPath)
        outputDirs.foreach(outputDir => {
            FileSystem.get(sc.hadoopConfiguration).delete(new Path(outputDir), true)
        })
        val bitcoin_blocks_path = "bitcoin_blocks_v2"

        val btcUsdDataPath = "Coinbase_BTCUSD.pq"
        val spark = SparkSession.builder.getOrCreate
        import spark.implicits._
        var btc_usd_rdd = spark.read.parquet(btcUsdDataPath).rdd
                                .map(x => (x(0).toString.toLong,(x(1).toString.toDouble, x(2).toString.toDouble,
                                x(3).toString.toDouble, x(4).toString.toDouble, x(5).toString.toDouble,
                                x(6).toString, x(7).toString.toDouble))
                                )
                                // (unix,(low,high,open,close,volume,date,vol_fiat))
        
        val btcUsdRDDMapBroadcast = sc.broadcast(btc_usd_rdd.collectAsMap)
      
        val rcvrAccountsPath = List("rcvr_addresses_with_acc_65.txt", "rcvr_addresses_with_acc_70.txt", "rcvr_addresses_with_acc_75.txt", "rcvr_addresses_with_acc_80.txt", "rcvr_addresses_with_acc_90.txt")
        val rcvrAccountsMapping = Map[String, Set[String]]()
        rcvrAccountsPath.foreach(rcvrAccountPath => {
            val fileLines = Source.fromFile(rcvrAccountPath).getLines.toList
            val rcvrAddresses = fileLines.map(x => x.substring(1, x.length-1).split(",")(0)).toSet
            rcvrAccountsMapping(rcvrAccountPath) = rcvrAddresses
        })

        var binaryFiles = sc.binaryFiles(bitcoin_blocks_path)

        var blocks = binaryFiles.flatMap(binaryFile => {
            var blocks = readBlocks(binaryFile._2.open(), List())
            blocks
        })

        var blockLevelData = blocks.map(block => {
            val blockId = block.header.blockId.toHex
            val blockFloorTime = (block.header.time / 60)*60 // blockFloorTime is block timestamp truncated to minute-level granularity
            val receivers = block.tx.flatMap(txn => txn.txOut.map(txOut => determinePubKeyHash(txOut.publicKeyScript.toHex))).toSet
            val numUniqueReceivers = receivers.size
            val txns = block.tx
            val numTxnsInBlock = txns.length
            val coinbaseTxn = txns(0)
            val totalMinerReward = coinbaseTxn.txOut(0).amount.toBtc.toDouble // miner's reward + fee reward
            val blockFeeReward = totalMinerReward - 6.25
            val totalBtcAmountReceivedInBlock = block.tx.map(txn => txn.txOut.map(txOut => txOut.amount.toBtc.toDouble).sum).sum
            
            val proportionOfReceiversWithXAccInBlockData = rcvrAccountsPath.map(rcvrAccountPathWithXAcc => {
                val rcvrAccountsWithXAcc = rcvrAccountsMapping(rcvrAccountPathWithXAcc)
                val numRcvrAccountsWithXAcc = rcvrAccountsWithXAcc.size
                var counter = 0
                rcvrAccountsWithXAcc.foreach(rcvrAccount => {
                    if (receivers.contains(rcvrAccount)) {
                        counter += 1
                    }
                })
                (1D*counter/numRcvrAccountsWithXAcc)
            })
            val proportionOfReceiversWith65AccInBlock = proportionOfReceiversWithXAccInBlockData(0)            
            val proportionOfReceiversWith70AccInBlock = proportionOfReceiversWithXAccInBlockData(1)
            val proportionOfReceiversWith75AccInBlock = proportionOfReceiversWithXAccInBlockData(2)
            val proportionOfReceiversWith80AccInBlock = proportionOfReceiversWithXAccInBlockData(3)
            val proportionOfReceiversWith90AccInBlock = proportionOfReceiversWithXAccInBlockData(4)
            
            val currBitcoinPrice = btcUsdRDDMapBroadcast.value(blockFloorTime)._4
            val bitcoinPrice1HrAgo = btcUsdRDDMapBroadcast.value(blockFloorTime-(1*60*60))._4
            val bitcoinPrice2HrAgo = btcUsdRDDMapBroadcast.value(blockFloorTime-(2*60*60))._4
            val bitcoinPrice3HrAgo = btcUsdRDDMapBroadcast.value(blockFloorTime-(3*60*60))._4
            val bitcoinPrice4HrAgo = btcUsdRDDMapBroadcast.value(blockFloorTime-(4*60*60))._4
            val avgAmtOfBtcPerTxn = totalBtcAmountReceivedInBlock / numTxnsInBlock
            val numTxnsWithMoreThan10Outputs = block.tx.filter(tx => (tx.txOut.length >= 10)).length
            val numHighValueTxnsInBlock = block.tx.flatMap(tx => tx.txOut.map(txOut => txOut.amount.toBtc.toDouble)).filter(x => x>=10).length
            // NOTE: High Value Txn defined as when a sinle receiver addr receives >= 10 BTC in a transaction

            // TODO: Get num txns in block where a receiver receives more than 1 btc
            val bitcoinPrice4HrLater = btcUsdRDDMapBroadcast.value(blockFloorTime+(4*60*60))._4
            val label = if (bitcoinPrice4HrLater > currBitcoinPrice) 1 else 0
            val priceDiffBtw4HrLaterAndCurrPrice = bitcoinPrice4HrLater - currBitcoinPrice
            (blockId, blockFloorTime, numUniqueReceivers, numTxnsInBlock, blockFeeReward, 
            totalBtcAmountReceivedInBlock, proportionOfReceiversWith65AccInBlock, proportionOfReceiversWith70AccInBlock,
            proportionOfReceiversWith75AccInBlock, proportionOfReceiversWith80AccInBlock, proportionOfReceiversWith90AccInBlock,
            currBitcoinPrice, bitcoinPrice1HrAgo, bitcoinPrice2HrAgo, bitcoinPrice3HrAgo, bitcoinPrice4HrAgo,
            avgAmtOfBtcPerTxn, numTxnsWithMoreThan10Outputs, numHighValueTxnsInBlock, bitcoinPrice4HrLater, label, priceDiffBtw4HrLaterAndCurrPrice)
        }).sortBy(x => x._2, true)


        var df = blockLevelData.toDF("blockId", "blockFloorTime", "numUniqueReceivers", "numTxnsInBlock", "blockFeeReward", 
            "totalBtcAmountReceivedInBlock", "proportionOfReceiversWith65AccInBlock", "proportionOfReceiversWith70AccInBlock",
            "proportionOfReceiversWith75AccInBlock", "proportionOfReceiversWith80AccInBlock", "proportionOfReceiversWith90AccInBlock",
            "currBitcoinPrice", "bitcoinPrice1HrAgo", "bitcoinPrice2HrAgo", "bitcoinPrice3HrAgo", "bitcoinPrice4HrAgo",
            "avgAmtOfBtcPerTxn", "numTxnsWithMoreThan10Outputs", "numHighValueTxnsInBlock", "bitcoinPrice4HrLater", "label", "priceDiffBtw4HrLaterAndCurrPrice")
        
        // val windowSpec  = Window.orderBy("blockFloorTime")
        // df = df.withColumn("numTxns1BlockAgo", lag("numTxnsInBlock", 1).over(windowSpec))
        // df = df.withColumn("numTxns2BlocksAgo", lag("numTxnsInBlock", 2).over(windowSpec))
        // df = df.withColumn("numTxns3BlocksAgo", lag("numTxnsInBlock", 3).over(windowSpec))
        // df = df.withColumn("numTxns4BlocksAgo", lag("numTxnsInBlock", 4).over(windowSpec))
        // df = df.withColumn("totalBtcAmountReceived1BlockAgo", lag("totalBtcAmountReceivedInBlock", 1).over(windowSpec))
        // df = df.withColumn("totalBtcAmountReceived2BlocksAgo", lag("totalBtcAmountReceivedInBlock", 2).over(windowSpec))
        // df = df.withColumn("totalBtcAmountReceived3BlocksAgo", lag("totalBtcAmountReceivedInBlock", 3).over(windowSpec))
        // df = df.withColumn("totalBtcAmountReceived4BlocksAgo", lag("totalBtcAmountReceivedInBlock", 4).over(windowSpec))
        var df_train = df.limit(3800).toDF
        var df_valid_and_test = df.except(df_train).toDF
        var df_valid = df_valid_and_test.limit(500).toDF
        var df_test = df_valid_and_test.except(df_valid).toDF


        df.coalesce(1).write.option("header",true).parquet(allDataPath)
        // df_train.write.option("header",true).parquet(allDa)
        // df_valid_and_test.coalesce(1).write.option("header", true).parquet(testDataPath)
        df_train.coalesce(1).write.option("header", true).parquet(trainDataPath)
        df_valid.coalesce(1).write.option("header", true).parquet(validDataPath)
        df_test.coalesce(1).write.option("header", true).parquet(testDataPath)

        /* NOTE: How to read saved data:
        val df_train = spark.read.format("csv").option("header", true).load("block_train_data")
        val df_test = spark.read.format("csv").option("header", true).load("block_test_data")
        */
  }
}
