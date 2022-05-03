package StockAnalysisPackage
import org.apache.spark.sql.SparkSession

import StockAnalysisPackage.RuntimeParameters

trait Stock {
  def generateReport(spark: SparkSession, runtimeParameters: RuntimeParameters)
}
