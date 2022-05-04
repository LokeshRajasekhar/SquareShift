package StockAnalysisPackage


import org.apache.spark
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Encoder, Encoders}

object StockAnalysis extends Stock{

  override def generateReport(spark: SparkSession, runtimeParameters: RuntimeParameters): Unit = {
    val start_date =runtimeParameters.start_Date
    val end_date = runtimeParameters.end_Date
    val sectors= runtimeParameters.sector

    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "")
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey ", "")
    spark.sparkContext.hadoopConfiguration.set("com.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.endpoint", "s3.ap-south-1.amazonaws.com")
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.aws.credentials.provider","org.apache.hadoop.fs.s3n.BasicAWSCredentialsProvider")

    val meta_df=spark.read.option("header","true").csv("s3n://squareshift-loki/dev/data/incoming/stock_analysis/symbol_metadata.csv")
    import spark.implicits._
    val list_meta=meta_df.select("Symbol").as[String].collect.toList


    var x: String=null

    var retdf  = spark.emptyDataFrame
    case class test(timestamp:String,open:Int,high:Int,low:Int,close:Int,volume:Int,company:String)
    var stock_df =spark.emptyDataset[test].toDF



    //function to read the dataframes from s3
    def company_stock_performance( company: String) : DataFrame = {
      retdf=spark.read.option("header","true").csv("/mnt/s3data/dev/data/incoming/stock_analysis/"+s"$company"+".csv")
      retdf
    }

    for (  x <- list_meta ){
      stock_df  = stock_df.union(company_stock_performance(x).withColumn("company",lit(x)))

    }

    //    val stock_sector_df=stock_df.as("t1").join(meta_df.as("t2"), "t1.company" == "t2.Symbol").select("t1.*","t2.Name","t2.Sector")
    stock_df.createOrReplaceTempView("stock_df")
    meta_df.createOrReplaceTempView("meta_df")

    val stock_sector_df=spark.sql("select t1.*,t2.Name,t2.Sector from stock_df t1 join meta_df t2 on t1.company = t2.Symbol ")

    stock_sector_df.createOrReplaceTempView("stock_sector")

    val final_query_summary_report = s"select Sector,round(avg(open),1) as Avg_Open_Price, round(avg (close),1) as Avg_Close_Price,round(max(high),1) as Max_High_Price,round(min(low),1) as Min_Low_Price, round(avg(volume),1) as Avg_Volume from stock_sector where date(timestamp) between date('${start_date}') and date('${end_date}') and sector in (${sectors.map ( x => "'" + x + "'").mkString(",") }) group by sector"
    val final_dataframe_summary_report_df= spark.sql(final_query_summary_report)



    val final_query_detailed_report=s"""select company as Symbol,Name,round(avg(open),1) as Avg_Open_Price, round(avg (close),1) as Avg_Close_Price,round(max(high),1) as Max_High_Price,round(min(low),1) as Min_Low_Price, round(avg(volume),1) as Avg_Volume from stock_sector where date(timestamp) between date('${start_date}') and date('${end_date}') and sector in (${sectors.map ( x => "'" + x + "'").mkString(",") }) group by  Symbol,Name"""
    val final_dataframe_detailed_report_df= spark.sql(final_query_detailed_report)

    val summary_report_count=final_dataframe_summary_report_df.count()
    val detailed_report_count=final_dataframe_detailed_report_df.count()


    if (summary_report_count==0)
    {
      println("The detailed_report is having 0 data for the given period and sector")
      System.exit(0)
    }else if  (detailed_report_count==0)
    {
      println("The summary_report is having 0 data for the given period and sector")
      System.exit(0)
    }

    final_dataframe_detailed_report_df.write.option("header", "true").mode("overwrite").csv("s3n://squareshift-loki/dev/data/output/detailed_report")
    final_dataframe_summary_report_df.write.option("header", "true").mode("overwrite").csv("s3n://squareshift-loki/dev/data/output/summary_report")
    import spark.implicits._

  }
}
