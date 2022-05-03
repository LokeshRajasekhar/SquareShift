import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

val start_date = "2021-01-01"
val end_date = "2021-05-26"
val sectors= Seq("TECHNOLOGY","FINANCE")
val meta_df=spark.read.option("header","true").csv("/home/ec2-user/dev/data/symbol_metadata.csv")
val list_meta=meta_df.select("Symbol").map(f=>f.getString(0)).collect.toList


val schema = StructType(
      StructField("timestamp", StringType, false) ::
      StructField("open", IntegerType, false) ::
      StructField("high", IntegerType, false) ::
          StructField("low", IntegerType, false) ::
      StructField("close", IntegerType, false) ::
      StructField("volume", IntegerType, false) ::
          StructField("company", StringType, false) ::Nil)


var retdf  = spark.emptyDataFrame
var stock_df  = spark.createDataFrame(sc.emptyRDD[Row], schema)



//function to read the dataframes from s3
def company_stock_performance( company: String) : DataFrame = {
        retdf=spark.read.option("header","true").csv("/home/ec2-user/dev/data/"+s"$company"+".csv")
        retdf
}

for (  x <- list_meta ){
        stock_df  = stock_df.union(company_stock_performance(x).withColumn("company",lit(x)))

}



val stock_sector_df=stock_df.as("t1").join(meta_df.as("t2"), $"t1.company" === $"t2.Symbol").select($"t1.*",$"t2.Name",$"t2.Sector")

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

final_dataframe_detailed_report_df.write.option("header", "true").mode("overwrite").csv("/home/ec2-user/dev/data/output/detailed_report")
final_dataframe_summary_report_df.write.option("header", "true").mode("overwrite").csv("/home/ec2-user/dev/data/output/summary_report")


//spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "")
//spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey ", "")
//spark.sparkContext.hadoopConfiguration.set("com.amazonaws.services.s3.enableV4", "true")
//spark.sparkContext.hadoopConfiguration.set("fs.s3n.endpoint", "s3.ap-south-1.amazonaws.com")
//spark.sparkContext.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
//spark.sparkContext.hadoopConfiguration.set("fs.s3n.aws.credentials.provider","org.apache.hadoop.fs.s3n.BasicAWSCredentialsProvider")

//final_dataframe_detailed_report_df.write.csv("s3://squareshift-loki/dev/data/output/detailed_report")
//final_dataframe_summary_report_df.write.csv("s3n://squareshift-loki/dev/data/output/summary_report")


//System.exit(0)