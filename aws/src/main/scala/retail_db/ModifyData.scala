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



object ModifyData {
  def main(args: Array[String]): Unit = {
    val props = ConfigFactory.load()
    val envProps = props.getConfig(args(0))
    val load_type = args(1)
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
      csv(inputBaseDir + "orders" )

    val outputBaseDir = envProps.getString("output.base.dir")
    val outputDeltaBaseDir = envProps.getString("output.delta.dir")
    
    orders.
      write.
      mode("overwrite").
      json(outputBaseDir + "orders")

      spark.read.json(outputBaseDir + "orders").take(10).foreach(println)
      
//    Write output in DELTA Format
      // 0 ==> Full Load
      // 1 ==> Append
      // 2 ==> Merge
      
      println("load_type="+load_type)
        val deltaTable = DeltaTable.forPath(outputDeltaBaseDir + "orders")
        val deltaTableDF = spark.read.format("delta").load(outputDeltaBaseDir + "orders")

        
      if (load_type == "0") { //Full load
          orders.
          write.
          mode("overwrite").
          format("delta").
          save(outputDeltaBaseDir + "orders")
      } else if (load_type == "1"){ // Append
          orders.
          write.
          mode("append").
          format("delta").
          save(outputDeltaBaseDir + "orders")                
      } else if (load_type == "2"){ // Merge Or Upsert     
      
        deltaTable.as("oldData")
        .merge(
         orders.as("newData"),
         "oldData.order_id = newData.order_id")
         // This is first WhenMatched Update. There can be pre-condition before an Update can happen.
         // If multiple rows of source table matches with multiple rows of target table for a PK, merge can fail as it's ambiguous.
        //.whenMatched                               // whenMatched without optional condition  
     
        /* 
        .whenMatched("newData.order_id = 2")     // whenMatched with optional condition
        .update(Map(
                    "order_id" -> col("newData.order_id"),
                    "order_date" -> col("newData.order_date")
                   ))*/

        .whenMatched("newData.order_id > 2").updateAll()   // This is second WhenMatched Update.                 
        
        .whenNotMatched            // This is first WhenNotMatched Update.
        .insert(Map("order_id" -> col("newData.order_id"))) 
        // This will only insert Order id to Target. 
        //Other columns in Target will be null.                                                        
        //.whenNotMatched.insertAll() ==> This inserts all columns from Source to Target.        
        .execute()
        
      }

      deltaTable.generate("symlink_format_manifest")
      deltaTable.toDF.show()
      println("Total record count $ " + deltaTable.toDF.count())
      
      deltaTableDF.take(10).foreach(println) 
       
  }

}