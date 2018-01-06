package com.vora.documentengine.csv;

import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * JSON数据源
 * Run spark job submit 
 * vorahost:~ 
 * # spark-submit --jars /opt/vora/lib/vora-spark/lib/spark-sap-datasources-1.4.2.20-vora-1.4-assembly.jar 
 *                --class come.documentengine.json.CSVataSourceReviewsVoraHost vorademo.jar
 * @author Yanbing
 */
public class CSVataSourceReviewsVoraHost {

	/**
	 * Avoid getting following error:
	 * 			java.net.ConnectException: Connection refused: vorahost/xxx.xxx.xxx.xxx:7077
	 * Start spark master and worker:
	 * 			start-master.sh
	 * 			start-slave.sh vorahost:7077
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("JSONDataSource")
				.setMaster("spark://vorahost:7077");
		
		//Config vora host path and file location in HDFS
		conf.set("fs.defaultFS", "hdfs://vorahost:9000");  
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);		
		
		HashMap<String, String> options = new HashMap<String, String>();
		
		options.put("header", "true");
		options.put("path", "/usr/vora/products.csv");
		
		StructType customProductsSchema = new StructType(new StructField[] {
			    new StructField("model", DataTypes.StringType, true, Metadata.empty()),
			    new StructField("type", DataTypes.StringType, true, Metadata.empty()),
			    new StructField("name", DataTypes.StringType, true, Metadata.empty()),
			    new StructField("quantity", DataTypes.IntegerType, true, Metadata.empty()),
			    new StructField("sale_date", DataTypes.DateType, true, Metadata.empty())
			});
		
		DataFrame productsDF = sqlContext.read()
			    .format("com.databricks.spark.csv")
			    .schema(customProductsSchema)
			    .option("header", "false")
			    .option("charset", "UTF8")
			    .load("products.csv");
		
		productsDF.show();
				
		//Use 127.0.0.1 instead of vorahost because vora server /etc/hosts resovles 127.0.0.1 first
		productsDF.write().format("json").save("hdfs://127.0.0.1:9000/output/newProducts");  
		
		sc.stop();
	}
	
}
