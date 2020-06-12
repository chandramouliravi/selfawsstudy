package retail_db

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


object CreateDeltaTable {
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
      
      spark.sql("create table events using delta location '/tmp/events'")
       
  }
}