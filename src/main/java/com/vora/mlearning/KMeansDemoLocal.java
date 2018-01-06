package com.vora.mlearning;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.vora.model.SalesCase;
import com.vora.model.SalesCaseKmViewModel;
import com.vora.utils.PrintUtil;

/**
 * K-Means属于基于平方误差的迭代重分配聚类算法，其核心思想如下：
 * 1.随机选择K个中心点
 * 2.计算所有点到这K个中心点的距离，选择距离最近的中心点为其所在的簇
 * 3.简单的采用算术平均数（mean）来重新计算K个簇的中心
 * 4.重复步骤2和3,直至簇类不在发生变化或者达到最大迭代值
 * 5.输出结果
 * K-Means算法的结果好坏依赖于对初始聚类中心的选择，容易陷入局部最优解，对K值的选择没有准则可依循，对异常数据较为敏感，只能处理数值属性的数据，聚类结构可能不平衡。
 * @author Yanbing
 */
public class KMeansDemoLocal {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("KMeansDemoLocal")
				.setMaster("local");  
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);	
		
		HashMap<String, String> options = new HashMap<String, String>();
		options.put("header", "true");
		options.put("path", "./data/kmeans_input_data.csv");
		
		StructType salesCaseSchema = new StructType(new StructField[] {
			    new StructField("PARTNERID", DataTypes.StringType, true, Metadata.empty()),
			    new StructField("TOTAL_SALESAMOUNT", DataTypes.DoubleType, true, Metadata.empty()),
			    new StructField("NOOFITEMS", DataTypes.DoubleType, true, Metadata.empty()),
			    new StructField("REGION", DataTypes.IntegerType, true, Metadata.empty())
		});
		
		DataFrame salesCaseDF = sqlContext.read()
			    .format("com.databricks.spark.csv")
			    .schema(salesCaseSchema)
			    .option("header", "true")
			    .option("charset", "UTF8")
			    .load("./data/kmeans_input_data.csv");
		
		salesCaseDF.show();

		// DataFrame converts to RDD
		JavaRDD<Row> rowRDD = salesCaseDF.javaRDD();	
		
		// Convert Row RDD to SalesCase RDD
		JavaRDD<SalesCase> salesCaseRDD = rowRDD.map(new Function<Row, SalesCase>() {		
			private static final long serialVersionUID = 1L;
			@Override
			public SalesCase call(Row row) throws Exception {
				SalesCase salesCase = new SalesCase();
				salesCase.setId(row.getString(0));
				salesCase.setTotalSalesAmount(row.getDouble(1));
				salesCase.setNoOfItems(row.getDouble(2));
				salesCase.setRegion(row.getInt(3));
				System.out.println(salesCase);
				return salesCase;
			}
		});

		//Convert to RDD without rowId. This will be the input to the KMeans algorithm
		JavaRDD<Vector> vectors = salesCaseRDD.map(s -> {
			return Vectors.dense(s.getTotalSalesAmount(), s.getNoOfItems(), s.getRegion());
		});
		
		//Cache RDD
		vectors.cache();
		
		
		int numClusters = 10; //预测分为10个簇类
		int numIterations = 30; //迭代30次
		int runs = 10; 	//运行10次，选出最优解
		
		KMeansModel clusters = KMeans.train(vectors.rdd(), numClusters, numIterations, runs);
		
		//计算测试数据分别属于哪个簇类
		PrintUtil.print(vectors.map(v -> v.toString() + " belong to cluster :" + clusters.predict(v)).collect());
		
		//计算cost
		double wssse = clusters.computeCost(vectors.rdd());
		System.out.println("Within Set Sum of Squared Errors = " + wssse);

		//打印出中心点
		System.out.println("Cluster centers:");
		for (Vector center : clusters.clusterCenters()) {
			System.out.println(" " + center);
		}
				 
		JavaRDD<SalesCaseKmViewModel> kmViewRDD = salesCaseRDD.map(new Function<SalesCase, SalesCaseKmViewModel>() {
				private static final long serialVersionUID = 1L;
				@Override
				public SalesCaseKmViewModel call(SalesCase sc) throws Exception {
					SalesCaseKmViewModel s = new SalesCaseKmViewModel();
					s.setId(sc.getId());		
					s.setCluster(Integer.toString(clusters.predict(Vectors.dense(sc.getTotalSalesAmount(),sc.getNoOfItems(),sc.getRegion()))));
					return s;
				}
		});
				
	    DataFrame clusterDF = sqlContext.createDataFrame(kmViewRDD, SalesCaseKmViewModel.class);
		
	    //Create temporary table from the df and display the results 
		clusterDF.registerTempTable("CLUSTER_MAPPING");
		clusterDF.show();
			
		sc.stop();
		 
	}
	
}
