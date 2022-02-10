package org.anji.spark;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Demo of Simple dataset Operation 
 * @author anjinappa
 *
 */
public class SparkDatasetOps {

	public static void main(String[] args) {
		
	
		/**
		 * Creating SparkSession Object 
		 */
		SparkSession spark = SparkSession.builder().master("local").appName("spark-example-dataset").getOrCreate();

		String path = "src/resources/input/edg_trade.csv";

		Map<String, String> optionsMap = new HashMap<String, String>();
		optionsMap.put("delimiter",",");
		optionsMap.put("header","true");
		optionsMap.put("inferSchema", "true");
		optionsMap.put("mode", "DROPMALFORMED");
		
		/**
		 * Reading from csv data and printing 
		 */
		Dataset<Row> dataset = spark.read().options(optionsMap).csv(path);
		dataset.printSchema();
		dataset.show();
		
		System.out.println("****Displaying the Only Buy trades ***** ");
		
		dataset= dataset.filter( dataset.col("TRADE_TYPE").eqNullSafe("BUY"));
		dataset.show();
		/**
		 * Dataset transformation 
		 */
		Dataset<Row> newDataSet = dataset.filter(dataset.col("TRADER_NAME").eqNullSafe("NIFTY 500"));
		
		newDataSet.write().option("compression", "none").format("parquet").mode(SaveMode.Overwrite).save("src/resources/output/parquet/");
		
		System.out.println("****Converting Dataset to Parquet File***** ");
		
		
		System.out.println("Reading and Displaying only Parquet file Content");
		
		Dataset<Row> parquetData= spark.read().option("recursiveFileLookup", "true").parquet("src/resources/output/parquet/");
		
		/**
		 * Creating temp view or table from Dataset Stream .
		 *
		 */
		parquetData.createOrReplaceTempView("EdgTrade");

		parquetData.show();
		
		parquetData = parquetData.select("*").where("QUANTITY=2");
		
		System.out.println("-----------Sql Print------");
		parquetData.show();
		
		System.out.println("*****Sql using Spark Context or SparkSession");
		spark.sql("select * from EdgTrade where QUANTITY=3").show();
		
	    /**
	     * Closing Spark Session 
	     */
		spark.close();
		
		
		

	}

}
