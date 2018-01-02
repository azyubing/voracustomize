package com.vora.custom.udf;


import java.sql.Date;
import java.time.LocalDate;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.vora.model.Person;


public class CalculateAgeUDF {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf()
				.setAppName("CalculateAgeUDF")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);		
		SQLContext sqlContext = new SQLContext(sc);		
		
		List<Person> list = new ArrayList<Person>();		
		list.add(new Person("Yarn",java.sql.Date.valueOf(LocalDate.parse("2013-01-01"))));
		list.add(new Person("Marry", java.sql.Date.valueOf(LocalDate.parse("1980-12-21"))));
		list.add(new Person("Jacky", java.sql.Date.valueOf(LocalDate.parse("1990-11-10"))));
		list.add(new Person("Tom", java.sql.Date.valueOf(LocalDate.parse("2001-02-03"))));
		list.add(new Person("Jerry", java.sql.Date.valueOf(LocalDate.parse("2002-03-10"))));
		JavaRDD<Person> personRdd = sc.parallelize(list);	
		JavaRDD<Row> rowRdd = personRdd.map(new Function<Person, Row>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Row call(Person person) throws Exception {
				
				return RowFactory.create(person.getName(),person.getBirthDate());
			}
		});
		
		
		ArrayList<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("birthDate", DataTypes.DateType, true));
		structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		StructType structType = DataTypes.createStructType(structFields);
		
		DataFrame nameDF = sqlContext.createDataFrame(rowRdd, structType);		
		nameDF.registerTempTable("personTable");
		
	    /**
		  * 根据UDF函数参数的个数来决定是实现哪一个UDF  UDF1，UDF2  ... UDF1xxx
		  */	
		sqlContext.udf().register("CalculateAge", new UDF1<Date, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Date birthDate) throws Exception {
				return getAge(birthDate);
			}
		}, DataTypes.IntegerType);
		
		sqlContext.sql("SELECT name, birthDate, CalculateAge(birthDate) as age from personTable").show();
		sc.stop();
	}
	


    public static Integer getAge(Date dob) {
    	    LocalDate localDobDate = dob.toLocalDate();
        LocalDate curDate = LocalDate.now();
        return Period.between(localDobDate, curDate).getYears();
    }

}
