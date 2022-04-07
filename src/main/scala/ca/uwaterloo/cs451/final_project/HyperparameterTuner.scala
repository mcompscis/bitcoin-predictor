
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
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.Pipeline



/*
A Script to get raw transaction level data and block-receiver acct level data

*/
class HyperparameterTunerConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(blockpath)
  val blockpath = opt[String](descr = "block file", required = false)
  verify()
}

object HyperparameterTuner {
        
    def main(argv: Array[String]): Unit = {
        val args = new HyperparameterTunerConf(argv)

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
           "numTxnsWithMoreThan10Outputs", "numHighValueTxnsInBlock"
        )
        
        val assembler = new VectorAssembler().setInputCols(features).setOutputCol("features")
        val trainFeatureDF = assembler.transform(df_train)
        val validFeatureDF = assembler.transform(df_valid)
        val testFeatureDF = assembler.transform(df_test)
        val randomForestClassifier = new RandomForestClassifier()

        val stages = Array(assembler, randomForestClassifier)
        val pipeline = new Pipeline().setStages(stages)

        val paramGrid = new ParamGridBuilder().addGrid(randomForestClassifier.numTrees, Array(100, 200, 500)).addGrid(randomForestClassifier.maxDepth, Array(4, 10, 20)).build()

        val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
        val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator) .setEstimatorParamMaps(paramGrid).setNumFolds(5)
        cv.setParallelism(4) 
        val cvModel = cv.fit(df_train)
        val cvPredictionDf = cvModel.transform(df_valid)
        val cvAccuracy = evaluator.evaluate(cvPredictionDf)

        println(cvAccuracy)

        val bestParams = cvModel.getEstimatorParamMaps.zip(cvModel.avgMetrics).maxBy(_._2)._1
        println(bestParams)

  }
}
