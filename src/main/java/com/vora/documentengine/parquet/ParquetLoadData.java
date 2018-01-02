package com.vora.documentengine.parquet;


import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * @author Yanbing
 */
public class ParquetLoadData {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("ProductsParquetLoadData")
				.setMaster("local");  
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		File coreSite = new File("/opt/hadoop-2.7.3/conf/core-site.xml");
		File hdfsSite = new File("/opt/hadoop-2.7.3/hdfs-site.xml");
		Configuration hConf = sc.hadoopConfiguration();
		hConf.addResource(new Path(coreSite.getAbsolutePath()));
		hConf.addResource(new Path(hdfsSite.getAbsolutePath()));
		
		SQLContext sqlContext = new SQLContext(sc);	
		
//		DataFrame productsDF = sqlContext.read().format("parquet").load("hdfs://vorahost:9000/usr/vora/products.parquet");
//		productsDF.registerTempTable("products");
//		productsDF.show();
//		productsDF.printSchema();
		
		DataFrame productsDF = sqlContext.read().parquet("hdfs://vorahost:9000/usr/vora/products.parquet");
		productsDF.registerTempTable("products");
		DataFrame resultDF = sqlContext.sql("SELECT * FROM products");
		resultDF.write().format("json").mode(SaveMode.Ignore).save("hdfs://vorahost:9000/output/products.parquet.json");
		
		
		sc.stop();
	}
	
}
