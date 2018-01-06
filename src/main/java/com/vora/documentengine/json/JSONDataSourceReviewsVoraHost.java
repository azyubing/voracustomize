package com.vora.documentengine.json;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * JSON数据源
 * Run spark job submit 
 * vorahost:~ 
 * # spark-submit --jars /opt/vora/lib/vora-spark/lib/spark-sap-datasources-1.4.2.20-vora-1.4-assembly.jar 
 *                --class com.documentengine.json.CSVataSourceReviewsVoraHost vorademo.jar
 * @author Yanbing
 */
public class JSONDataSourceReviewsVoraHost {

	/**
	 * Avoid getting following error:
	 * 			java.net.ConnectException: Connection refused: vorahost/xxx.xxx.xxx.xxx:7077
	 * Start spark master and worker:
	 * 			start-master.sh
	 * 			start-slave.sh vorahost:7077
	 * 
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("JSONDataSource")
				.setMaster("spark://vorahost:7077");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);		
		
		//Config vora host path and file location in HDFS
		conf.set("fs.defaultFS", "hdfs://vorahost:9000");  
		DataFrame reviewsDF = sqlContext.read().json("/usr/vora/reviews.json");  

		reviewsDF.registerTempTable("product_reviews");
		DataFrame reviewsDFById = sqlContext.sql(
				"select Document_id DOCUMENT_ID,"
				+ "product_id PRODUCT_ID, "
				+ "product_name PRODUCT_NAME, "
				+ "post_date POST_DATE, "
				+ "customer_details.id CUSTOMER_ID, "
				+ "customer_details.name CUSTOMER_NAME from product_reviews where _id>=10");

		reviewsDFById.show();		
		
		//Use 127.0.0.1 instead of vorahost because vora server /etc/hosts resovles 127.0.0.1 first
		reviewsDFById.write().format("json").save("hdfs://127.0.0.1:9000/output/reviews");  
		
		sc.stop();
	}
	
}
