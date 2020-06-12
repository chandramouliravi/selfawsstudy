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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.serializer.JavaSerializer 
import org.apache.spark.sql.{SaveMode, SparkSession, DataFrame}
import org.apache.spark.sql.delta.DeltaLog

import io.delta.tables._
import io.delta.tables.execution._
import io.delta.sql._
import io.delta.sql.parser._

import java.sql.Date
import java.text._
import org.apache.spark.sql.delta.DeltaTable

    
    
object SCDType2 {
  
 case class CustomerUpdate(customerId: Int, address: String, effectiveDate: Date)
 case class Customer(customerId: Int, address: String, current: Boolean, effectiveDate: Date, endDate: Date)
 
  def main(args: Array[String]): Unit = {
    implicit def date(str: String): Date = Date.valueOf(str)
    
    val props = ConfigFactory.load()
    val envProps = props.getConfig(args(0))
    var sparkSes:org.apache.spark.sql.SparkSession = null;

    if (args(0)=="dev") {
             sparkSes = SparkSession.
                builder.
                appName("Daily Product Revenue").
                master(envProps.getString("execution.mode")).
                getOrCreate()
    } else {
             sparkSes = SparkSession.
                builder.
                appName("Daily Product Revenue").
                master(envProps.getString("execution.mode")).
                config("hive.metastore.client.factory.class","com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").
                config("spark.sql.catalogImplementation","hive").
                enableHiveSupport().                    
                getOrCreate()
    }
          
    val spark = sparkSes;
    import spark.sqlContext.implicits._

     
    val sqlContext=spark.sqlContext
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.serializer","org.apache.spark.serializer.JavaSerializer")
    spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

   
    val inputBaseDir = envProps.getString("input.base.dir")
    val outputBaseDir = envProps.getString("output.base.dir")
    val outputDeltaBaseDir = envProps.getString("output.delta.dir")

    spark.sql("use default").show()
    spark.sql("show tables").show()
    spark.sql("drop table if exists customers")
   
    Seq(
      Customer(1, "old address for 1", false, null, "2018-02-01"),
      Customer(1, "current address for 1", true, "2018-02-01", null),
      Customer(2, "current address for 2", true, "2018-02-01", null),
      Customer(3, "current address for 3", true, "2018-02-01", null)
    //).toDF().write.format("delta").mode("overwrite").save(outputDeltaBaseDir + "customers")
    ).toDF().write.format("delta").mode("overwrite").saveAsTable("customers")
    // Created in file:/C:/Users/chandra/workspace-jun08/aws/spark-warehouse/customers in local mode
   
    spark.sql("select * from customers").show()
    
    Seq(
      CustomerUpdate(1, "new address for 1", "2018-03-03"),
      CustomerUpdate(3, "current address for 3", "2018-04-04"),    // new address same as current address for customer 3
      CustomerUpdate(4, "new address for 4", "2018-04-04")
    ).toDF().createOrReplaceTempView("updates")

    spark.sql("select * from updates").show()
    
    val customersTable = spark.sql("select * from customers")   // table with schema (customerId, address, current, effectiveDate, endDate)
    //val customersTable: DeltaTable = DeltaTable.forName("customers")
        
    val updatesDF = spark.sql("select * from updates")          // DataFrame with schema (customerId, address, effectiveDate)

// Rows to INSERT new addresses of existing customers
    val newAddressesToInsert = updatesDF
    .as("updates")
    .join(customersTable.as("customers"), "customerid")
    .where("customers.current = true AND updates.address <> customers.address")

// Stage the update by unioning two sets of rows
// 1. Rows that will be inserted in the `whenNotMatched` clause
// 2. Rows that will either UPDATE the current addresses of existing customers or INSERT the new addresses of new customers
    val stagedUpdates = newAddressesToInsert
      .selectExpr("NULL as mergeKey", "updates.*")   // Rows for 1.
      .union(
        updatesDF.selectExpr("updates.customerId as mergeKey", "*")  // Rows for 2.
      )

  /*
    customersTable
      .as("customers")
      .merge(
        stagedUpdates.as("staged_updates"),
        "customers.customerId = mergeKey")
      .whenMatched("customers.current = true AND customers.address <> staged_updates.address")
      .updateExpr(Map(                                      // Set current to false and endDate to source's effective date.
        "current" -> "false",
        "endDate" -> "staged_updates.effectiveDate"))
      .whenNotMatched()
      .insertExpr(Map(
        "customerid" -> "staged_updates.customerId",
        "address" -> "staged_updates.address",
        "current" -> "true",
        "effectiveDate" -> "staged_updates.effectiveDate",  // Set current to true along with the new address and its effective date.
        "endDate" -> "null"))
      .execute() 
  */
  
 } // end of main

} // end of Object