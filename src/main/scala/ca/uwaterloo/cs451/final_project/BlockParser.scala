
package ca.uwaterloo.cs451.final_project

import fr.acinq.bitcoin.Block
import fr.acinq.bitcoin.Protocol._
import java.io.{File, FileInputStream, ByteArrayInputStream, InputStream}
import scala.annotation.tailrec 
import org.rogach.scallop._

class BlockParserConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(blockfile)
  val blockfile = opt[String](descr = "block file", required = false)
  verify()
}

object BlockParser {
    
    def readBlock(input: InputStream): Block = {
        val magic = uint32(input)
        assert(magic == 0xd9b4bef9L)
        val size = uint32(input)
        val raw = new Array[Byte](size.toInt)
        input.read(raw)
        val block = Block.read(new ByteArrayInputStream(raw))
        block
    }

    @tailrec
    def readBlocks(input: InputStream, blocks: List[Block]): List[Block] = {
        if (input.available() <= 0) {
            blocks
        } else {
            val block = readBlock(input)
            readBlocks(input, block :: blocks)
        }
    }

    def main(argv: Array[String]): Unit = {
        val args = new BlockParserConf(argv)

        val filename = "bitcoin_blocks/blk02942.dat"

        val istream = new FileInputStream(filename)
        var blocks = readBlocks(istream, List())
        println(blocks.length)
        blocks = blocks.sortBy(_.header.time)
        blocks.foreach(x => println(x.blockId))

  }
}
