
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

class BlockParserSparkConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(blockpath)
  val blockpath = opt[String](descr = "block file", required = false)
  verify()
}

object BlockParserSpark {

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
        val args = new BlockParserSparkConf(argv)

        val conf = new SparkConf().setAppName("Bitcoin Block Parser")
        val sc = new SparkContext(conf)

        val outputDir = "transactions"

        FileSystem.get(sc.hadoopConfiguration).delete(new Path(outputDir), true)

        var binaryFiles = sc.binaryFiles("bitcoin_blocks")
        
        var blocks = binaryFiles.flatMap(binaryFile => {
            var blocks = readBlocks(binaryFile._2.open(), List())
            blocks
        })
        
        var txns = blocks.flatMap(block => {
            block.tx.map(x => (block.header.time, x.txid))
        })
        println("MINIMUM TRANSACTION TIME: " + txns.takeOrdered(1)(Ordering[Long].on(x => x._1))(0))
        println("MAXIMUM TRANSACTION TIME: " + txns.top(1)(Ordering[Long].on(x => x._1))(0))

        txns.saveAsTextFile(outputDir)

  }
}
