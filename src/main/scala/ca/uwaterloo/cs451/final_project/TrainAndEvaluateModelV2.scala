
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
// import spark.implicits._
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.evaluation.MulticlassMetrics
/*
A Script to get raw transaction level data and block-receiver acct level data

*/
class TrainAndEvaluateModelV2Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(blockpath)
  val blockpath = opt[String](descr = "block file", required = false)
  verify()
}

object TrainAndEvaluateModelV2 {
        
    def main(argv: Array[String]): Unit = {
        val args = new TrainAndEvaluateModelV2Conf(argv)

        val conf = new SparkConf().setAppName("Raw Data Bitcoin Parser")
        val sc = new SparkContext(conf)
        val spark = SparkSession.builder.getOrCreate
        
        spark.sparkContext.setLogLevel("ERROR")
        import spark.implicits._
        val trainDataPath = "train_bitcoin_blocks_data"
        val validDataPath = "valid_bitcoin_blocks_data"
        val testDataPath = "test_bitcoin_blocks_data"
        
        var df_train = spark.read.format("parquet").option("header", true).load(trainDataPath)
        val df_valid = spark.read.format("parquet").option("header", true).load(validDataPath)
        var df_test = spark.read.format("parquet").option("header", true).load(testDataPath)

        val features = Array(
          "numUniqueReceivers", 
          "numTxnsInBlock", 
          // "blockFeeReward", 
          "totalBtcAmountReceivedInBlock", 
           "proportionOfReceiversWith65AccInBlock", "proportionOfReceiversWith70AccInBlock",
           "proportionOfReceiversWith75AccInBlock", "proportionOfReceiversWith80AccInBlock",
            "proportionOfReceiversWith90AccInBlock", 
            "currBitcoinPrice", 
            "bitcoinPrice1HrAgo", "bitcoinPrice2HrAgo", "bitcoinPrice3HrAgo", "bitcoinPrice4HrAgo",
          //  "avgAmtOfBtcPerTxn", 
           "numTxnsWithMoreThan10Outputs", "numHighValueTxnsInBlock"
          //  , "totalBtcAmountReceived1BlockAgo", "totalBtcAmountReceived2BlocksAgo",
          //  "totalBtcAmountReceived3BlocksAgo", "totalBtcAmountReceived4BlocksAgo",
          //  "numTxns1BlockAgo", "numTxns2BlocksAgo", "numTxns3BlocksAgo", "numTxns4BlocksAgo"
          //  "changeInBtcAmtBtwCurrAnd1BlockAgo", "changeInBtcAmtBtw1And2BlocksAgo",
          //  "changeInBtcAmtBtw2And3BlocksAgo", 
          //  "changeInBtcAmtBtw3And4BlocksAgo",
          //  "changeInNumTxnsBtwCurrAnd1BlockAgo", "changeInNumTxnsBtw1And2BlocksAgo",
          //  "changeInNumTxnsBtw2And3BlocksAgo", "changeInNumTxnsBtw3And4BlocksAgo",
          //  "changeInBtcPriceBtwCurrAnd1HrAgo", "changeInBtcPriceBtw1And2HrAgo"
           
          //  "changeInBtcPriceBtw2And3HrAgo", "changeInBtcPriceBtw3And4HrAgo"
        )
        
        val assembler = new VectorAssembler().setInputCols(features).setOutputCol("features")
        val trainFeatureDF = assembler.transform(df_train)
        val validFeatureDF = assembler.transform(df_valid)
        val testFeatureDF = assembler.transform(df_test)

        // val indexer = new StringIndexer()
        val randomForestClassifier = new RandomForestClassifier().setImpurity("gini").setMaxDepth(10).setNumTrees(200).setFeatureSubsetStrategy("auto").setSeed(1000)




        val randomForestModel = randomForestClassifier.fit(trainFeatureDF)
        val predictionDf = randomForestModel.transform(validFeatureDF)
        val prediction2Df = randomForestModel.transform(testFeatureDF)

        // val evaluator = new BinaryClassificationEvaluator()
        //                     .setLabelCol("label")
        
        // evaluator.evaluate(predictionDf)
        val preds1RDD = predictionDf.select("prediction", "label").rdd.map(x => (x(0).toString.toDouble, x(1).toString.toDouble))
        val preds2RDD = prediction2Df.select("prediction", "label").rdd.map(x => (x(0).toString.toDouble, x(1).toString.toDouble))
        List(preds1RDD, preds2RDD).foreach(predsRDD => {
          val validOrTest = if (predsRDD == preds1RDD) "valid" else "test"
          println("\n-----------")
          println(validOrTest)
          println("-----------\n")
          val metrics = new MulticlassMetrics(predsRDD)
          
          // Overall Statistics
          val accuracy = metrics.accuracy
          println("Summary Statistics")
          println(s"Accuracy = $accuracy")

          // Precision by label
          val labels = metrics.labels
          labels.foreach { l =>
          println(s"Precision($l) = " + metrics.precision(l))
          }

          // Recall by label
          labels.foreach { l =>
          println(s"Recall($l) = " + metrics.recall(l))
          }

          // False positive rate by label
          labels.foreach { l =>
          println(s"FPR($l) = " + metrics.falsePositiveRate(l))
          }

          // F-measure by label
          labels.foreach { l =>
          println(s"F1-Score($l) = " + metrics.fMeasure(l))
          }

          // Weighted stats
          println(s"Weighted precision: ${metrics.weightedPrecision}")
          println(s"Weighted recall: ${metrics.weightedRecall}")
          println(s"Weighted F1 score: ${metrics.weightedFMeasure}")
          println(s"Weighted false positive rate: ${metrics.weightedFalsePositiveRate}")

        })

  }
}
