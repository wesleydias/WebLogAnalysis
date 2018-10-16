import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger


object WebLogAnalysis {

  def main(args: Array[String]) {

    val logger = Logger.getLogger(getClass().getName())

    logger.info("Creating spark session...")
    val spark = SparkSession.builder.appName("Cook4j Application").getOrCreate()

    import spark.implicits._

    /*
    Assumptions
    ----------
    The data received every hour of 1 TB, and is Incremental load. Hence all write operations uses "Append" mode.
    Incase of Full load the Mode will change to "Overwrite".
    */

    logger.info("Reading raw text file...")
    val datasource = spark.read.format("text").option("sep", "\\s").load("/FileStore/tables/2015_07_22_mktplace_shop_web_log_sample.log")

    val revised_data = datasource.withColumn("time_created", regexp_extract(datasource("value"), "^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$", 1)).
      withColumn("elb", regexp_extract(datasource("value"), "^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$", 2)).
      withColumn("client_ip", regexp_extract(datasource("value"), "^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$", 3)).
      withColumn("client_port", regexp_extract(datasource("value"), "^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$", 4)).
      withColumn("backend_ip", regexp_extract(datasource("value"), "^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$", 5)).
      withColumn("backend_port", regexp_extract(datasource("value"), "^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$", 6)).
      withColumn("request_processing_time", regexp_extract(datasource("value"), "^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$", 7)).
      withColumn("backend_processing_time", regexp_extract(datasource("value"), "^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$", 8)).
      withColumn("response_processing_time", regexp_extract(datasource("value"), "^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$", 9)).
      withColumn("elb_status_code", regexp_extract(datasource("value"), "^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$", 10)).
      withColumn("backend_status_code", regexp_extract(datasource("value"), "^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$", 11)).
      withColumn("received_bytes", regexp_extract(datasource("value"), "^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$", 12)).
      withColumn("sent_bytes", regexp_extract(datasource("value"), "^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$", 13)).
      withColumn("request", regexp_extract(datasource("value"), "^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$", 14)).
      withColumn("user_agent", regexp_extract(datasource("value"), "^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$", 15)).
      withColumn("ssl_cipher", regexp_extract(datasource("value"), "^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$", 16)).
      withColumn("ssl_protocol", regexp_extract(datasource("value"), "^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$", 17))

    logger.info("Creating temp table and writing in parquet format for performance optimization with gzip compression...")
    revised_data.createOrReplaceTempView("weblogs")
    revised_data.write.format("parquet").mode("Append").option("compression", "gzip").save("/mnt/wesley/customer/paytm/")

    logger.info("Sessionizing the raw data for query analysis...")

    val sessionized_data = spark.sql("select *,min(cast(time_created as timestamp)) over (partition by client_ip order by time_created) as min_date,cast(time_created as timestamp) as create_date from weblogs order by client_ip,time_created")
    val revised_sessionized_data = sessionized_data.withColumn("req_url", regexp_extract(revised_data("request"), "^(\\S+) (\\S+) (\\S+)$", 2))
    revised_sessionized_data.createOrReplaceTempView("weblogs2")

    val revised_sessionized_data_new = spark.sql("select *,cast(create_date as long) - cast (min_date as long) as elapsed_sec,round((cast(create_date as long) - cast (min_date as long))/60) as elapsed_min,round((cast(create_date as long) - cast (min_date as long))/3600) as elapsed_hr from weblogs2")
    revised_sessionized_data_new.createOrReplaceTempView("weblogs3")
    revised_sessionized_data_new.write.format("parquet").mode("Overwrite").save("/mnt/wesley/customer/paytm_session/")

    /* 
    Data Modal
    ----------
    The Raw CSV data is split into 2 tables which can be queried by Front-end API.

    Recipe 
    ------------
    Schema{
    recipe_id,
    recipe_name,
    description
    }

    Ingredients
    -------------
    Schema{
    recipe_id,
    ingredient,
    active
    }

    This should satisfy the conditions where the user can navigates from recipe summary page(hence refrencing only recipe dataset) 
    to detailed recipe page onclick (referncing the Ingredients dataset) for any given recipe_id and also this same table can be used 
    to refrence back all the recipe using this ingedients
    */

    logger.info("Find the avg session time -  Result should be 9201")
    val df1 = spark.sql("select avg(elapsed_sec) from weblogs3 ")
    df1.write.format("parquet").mode("Overwrite").save("/mnt/wesley/customer/avg_session_time/")

    /*

    Top 3 result for the below query is as follows:

    https://paytm.com:443/shop/cart?channel=web&version=2	43412
    https://paytm.com:443/	25129
    https://paytm.com:443/shop/v1/frequentorders?channel=web&version=2	17462

    */

    logger.info("Find the unique url per session for IP/Vistors")
    val df2 = spark.sql("select a.req_url,count(*) as hits_session from (select distinct req_url,client_ip from weblogs3) a group by a.req_url order by hits_session desc")
    df2.write.format("parquet").mode("Overwrite").save("/mnt/wesley/customer/unique_url_session/")



    /*
    Result of the below query is as follows:

    54.251.151.39
    46.236.24.51
    52.74.219.71
    119.81.61.166
    117.104.239.89
    106.186.23.95
    192.0.84.33
    54.169.191.85

     */

    logger.info("Find the IP with longest session time")
    val df3 = spark.sql("select distinct client_ip from weblogs3 where elapsed_hr > 18")
    df3.write.format("parquet").mode("Overwrite").save("/mnt/wesley/customer/longest_session_ip/")


    /* 
    Creating a final table for Data Science use-case Analysis

    */

    val timeseries_data = spark.sql("select cast(date_format(create_date, \"y-MM-dd hh:mm\") as timestamp) as create_ts,req_url,count(*) as number_req from weblogs3 group by create_ts,req_url")
    val stocks = timeseries_data.select("create_ts", "req_url", "number_req")
    val stocks1 = stocks.withColumn("minutes", round(col("create_ts").cast("long") - to_timestamp(lit("2015-07-22T00:00:00.000+0000")).cast("long")) / 60).withColumn("label", col("number_req"))

    stocks1.createOrReplaceTempView("weblogs5")
    stocks1.write.format("parquet").mode("Overwrite").save("/mnt/wesley/customer/timeseries_data/")

    val datascience = spark.sql("select a.*,b.num_unique_url_access,max(a.elapsed_sec) over(partition by a.client_ip) as total_time_session from weblogs3 a join (select c.client_ip, count(*) as num_unique_url_access from ( select distinct client_ip,req_url from weblogs3) c group by c.client_ip) b where a.client_ip=b.client_ip")
    datascience.createOrReplaceTempView("weblogs6")
    datascience.write.format("parquet").mode("Overwrite").save("/mnt/wesley/customer/ds_raw_data/")

    spark.stop()
  }

}
