package retail_db

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit
import com.typesafe.config.ConfigFactory

import org.apache.log4j.{Level, Logger}


object deltaApp extends App{

      
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  rootLogger.setLevel(Level.INFO)


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

    val props = ConfigFactory.load()
//    val envProps = props.getConfig(args(0))
    val envProps = props.getConfig("dev") 
    val Spark  = SparkSession.
      builder.
      appName("Daily Product Revenue").
      master(envProps.getString("execution.mode")).
      getOrCreate()

    val inputBaseDir = envProps.getString("input.base.dir")
    

  //Reading a file
    val df = Spark.
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

      
  val outputDeltaBaseDir = envProps.getString("output.delta.dir")
        
  //Creating a table
  df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").
  save(outputDeltaBaseDir + "delta_daily_product_revenue")

  //Reading a table

  val df1 = Spark.read.format("delta").load(outputDeltaBaseDir + "delta_daily_product_revenue")
  // val df1 = Spark.read.format("delta").load(outputDeltaBaseDir + "delta_daily_product_revenue"@v1)
  // val df1 = Spark.read.option("versionAsof",1).format("delta").load(outputDeltaBaseDir + "delta_daily_product_revenue"@v1)
  // val df1 = Spark.read.format("delta").load(outputDeltaBaseDir + "delta_daily_product_revenue"@20120908000000000) -- yyyyMMddHHmmssSSS
  // val df1 = Spark.read.option("timestampAsOf", "2012-09-08").format("delta").load(outputDeltaBaseDir + "delta_daily_product_revenue"@v1)

  // select * from delta_table VERSION AS OF 1;
  // select * from delta_table@v1;
  // select * from delta_table TIMESTAMP AS OF '2012-09-08';
  // select * from delta_table@20120908000000000;
  // Compaction
  // spark.read.format(delta).repartition(4).write.option("dataChange","false").format("delta").mode("overwrite").save(delta_path)
  // retention
  // spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = false")
  // deltaTable.vacuum(retentionHours = 0)   -- Default is 7 days
  df1.show()

  
  
  Spark.stop()

 
}
  