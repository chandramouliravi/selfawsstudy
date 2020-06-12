package retail_db

/* cd C:\Users\chandra\workspace-jun08\aws\target
 * spark-submit \
 *       --master local \
 *       --class retail_db.GetDailyProductRevenue \
 *       --jars  ‪D:\Documents\Technical\EMRSparkScalaDemo\config-1.3.2.jar \ 
 *       C:\Users\chandra\workspace-jun08\aws\target\aws-0.0.1-SNAPSHOT.jar
 */

/* Arguments :
 * spark-submit 
 * --deploy-mode client --master yarn --class retail_db.GetDailyProductRevenue --jars s3://chandraemr/config-1.3.2.jar 
 * s3://chandraemr/aws-0.0.1-SNAPSHOT.jar 
 * prod
 */
/*
 * Created by chandra on Jun, 07, 2020
 */
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import io.delta.tables._
import io.delta.sql
import io.delta.sql.parser
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.serializer.JavaSerializer 
import org.apache.spark.sql.{SaveMode, SparkSession, DataFrame}
import org.apache.spark.sql.delta.DeltaLog



object GetDailyProductRevenue extends Serializable {
  def main(args: Array[String]): Unit = {
    val props = ConfigFactory.load()
    val envProps = props.getConfig(args(0))
    val spark = SparkSession.
      builder.
      appName("Daily Product Revenue").
      master(envProps.getString("execution.mode")).
      getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.serializer","org.apache.spark.serializer.JavaSerializer")

    import spark.implicits._

    val inputBaseDir = envProps.getString("input.base.dir")
    val orders = spark.
      read.
      schema("""
        order_id INT,
        order_date STRING,
        order_customer_id INT,
        order_status STRING
        """).
      csv(inputBaseDir + "orders")
    val orderItems = spark.
      read.
      option("inferSchema", "true").
      schema("""
        order_item_id INT,
        order_item_order_id INT,
        order_item_product_id INT,
        order_item_quantity INT,
        order_item_subtotal FLOAT,
        order_item_product_price FLOAT
        """).
      csv(inputBaseDir + "order_items")

    val dailyProductRevenue = orders.where("order_status in ('CLOSED', 'COMPLETE')").
      join(orderItems, $"order_id" === $"order_item_order_id").
      groupBy("order_date", "order_item_product_id").
      agg(round(sum($"order_item_subtotal"), 2).alias("revenue")).
      orderBy($"order_date", $"revenue" desc)

    dailyProductRevenue.printSchema()
    
    val outputBaseDir = envProps.getString("output.base.dir")
    val outputDeltaBaseDir = envProps.getString("output.delta.dir")
    
    
     dailyProductRevenue.
      write.
      mode("overwrite").
      json(outputBaseDir + "daily_product_revenue")
      
      spark.read.json(outputBaseDir + "daily_product_revenue").take(10).foreach(println)
    
     val deltadata = spark.read.json(outputBaseDir + "daily_product_revenue")

//    Write output in DELTA Format
      deltadata.
      write.
      mode("overwrite").
      format("delta").
      save(outputDeltaBaseDir + "delta_daily_product_revenue")
      
      spark.read.format("delta").load(outputDeltaBaseDir + "delta_daily_product_revenue").take(10).foreach(println)
      

// DELETE Functionality      
      val deltaTable = DeltaTable.forPath(spark, outputDeltaBaseDir + "delta_daily_product_revenue")
      val deltaTableDF = spark.read.format("delta").load(outputDeltaBaseDir + "delta_daily_product_revenue")

      println("Data before Delete # " + deltaTable.toDF.count())
      deltaTable.delete(col("revenue") === 2319.42) 
      
      println("Data after  Delete # " + deltaTable.toDF.count())
               
// UPDATE Functionality
      
      println("Data before update "+
              deltaTableDF.filter("revenue >= 100").count())
              
      deltaTable.update(                // predicate using Spark SQL functions and implicits
      col("revenue") >= 100,
      Map("revenue" -> lit(20),"order_item_product_id" -> lit(1)));
 
      println("Data after update with revenue >=100 "+
              deltaTableDF.select(deltaTableDF.col("*")).filter("revenue >= 100").count())

      println("Data after update with revenue = 20 "+
              deltaTableDF.select(deltaTableDF.col("*")).filter("revenue == 20").count())              
      
      deltaTableDF.take(10).foreach(println) 

   // Craete Manifest File
      deltaTable.generate("symlink_format_manifest")      
      
  }

}