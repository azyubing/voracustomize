package com.vora.documentengine.json;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * JSON数据源
 * @author Yanbing
 */
public class JSONDataSourceReviewsLocal {

	/**
	 * Avoid getting following error:
	 * Exception in thread "main" java.net.ConnectException: 
	 * 			Call From DLCMXXXXXXA/127.0.0.1 to vorahost:9000 failed on connection exception: 
	 *          java.net.ConnectException: Connection refused; 
	 *          For more details see:  http://wiki.apache.org/hadoop/ConnectionRefused
	 * Add following to VM arguments:
	 *          -DHADOOP_USER_NAME=root
	 * 		
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("JSONDataSourceReviewLocal")
				.setMaster("local");  

		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		DataFrame reviewsDF = sqlContext.read().json("./data/reviews.json");  
		reviewsDF.registerTempTable("product_reviews");
		DataFrame reviewsDFById = sqlContext.sql(
				"select Document_id DOCUMENT_ID,"
				+ "product_id PRODUCT_ID, "
				+ "product_name PRODUCT_NAME, "
				+ "post_date POST_DATE, "
				+ "customer_details.id CUSTOMER_ID, "
				+ "customer_details.name CUSTOMER_NAME from product_reviews where _id>=10");
		
		reviewsDFById.show();		
		sc.stop();
	}
	
}
