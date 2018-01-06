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
 * CSV数据源
 * @author Yanbing
 */
public class CSVDataSourceReviewsLocal {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("CSVDataSourceReviewLocal")
				.setMaster("local");  
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);	
		
		HashMap<String, String> options = new HashMap<String, String>();
		
		options.put("header", "true");
		options.put("path", "./data/products.csv");
		
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
			    .load("./data/products.csv");
		
		productsDF.show();
				
		productsDF.select("model", "name").write()
			.format("com.databricks.spark.csv")
			.option("header", "false")
			.save("newProducts.csv");
		
		sc.stop();
	}
	
}
