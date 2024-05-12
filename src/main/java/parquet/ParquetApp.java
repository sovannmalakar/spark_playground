package parquet;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.Type;
import org.apache.spark.SparkConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class ParquetApp {
    public static void main(String args[]) throws IOException {
//        String appName = "parquetOperation";
//        Parquet parquet = ParquetReaderUtils.getParquetData("src/main/resources/cars.parquet");
//        SparkConf conf = new SparkConf().setAppName(appName).setMaster("spark://master:7077");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        Dataset<Row> parquetFile = sqlContext.read().parquet("people.parquet");
//        List<SimpleGroup> sg = parquet.getData();
//        parquet.getSchema().get(0).getOriginalType();
//        parquet.getSchema().get(0).asPrimitiveType();
//        List<Type> typeList = parquet.getSchema();
//        for(int i =0; i < 10; i++){
//            System.out.println(sg.stream().sequential());
//        }
//        System.out.println(parquet.getSchema());

         SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> parquetDataset = spark.read().parquet("src/main/resources/cars.parquet");
        Long count = parquetDataset.count();
        System.out.println("Schema is --->>>  " + parquetDataset.schema());
        String columns[] = parquetDataset.columns();
        for (String s:columns
             ) {
            System.out.println("colums --->" + s);
        }
        Dataset<Row> modelValues = parquetDataset.select("model");

        String columName[] =  modelValues.columns();
        for (String s:columName
        ) {
            System.out.println("column name--->" + s);
        }
//        System.out.println( modelValues.head().toString());
//        List<Row> models = modelValues.head().toString()
//        for(Row r : models) {
//
//            System.out.println(r.toString());
//        }

    }

}
