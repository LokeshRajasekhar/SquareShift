package StockAnalysisPackage
import org.apache.spark.sql.SparkSession

class stockMainClass {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("StockAnalysisPackage")
      .enableHiveSupport()
      .config("immuta.spark.acl.assume.not.privileged", "true")
      .config("spark.hadoop.immuta.databricks.config.update.service.enabled", "false")
      .getOrCreate()

    StockAnalysis.generateReport(spark, createParameters(args) )

  }

  def createParameters(args: Array[String]): RuntimeParameters={

    val runtimeParameters= new RuntimeParameters
    runtimeParameters.start_Date=args(0)
    runtimeParameters.end_Date=args(1)
    runtimeParameters.sector=args(2)
    runtimeParameters
  }
}