package parquet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import java.io.IOException;





public  class ParquetApp {

    public static void main(String args[]) throws IOException {
        SparkSession sparkSession = SparkSession.builder().appName("PARQUET_APP").master("local").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().parquet("file:///C:/Users/sovan/project/parquet_file_operation/src/main/resources/Flights.parquet");
        dataset.show();
        Dataset<Row> modeDataset = dataset.select("DEP_DELAY");
        modeDataset.show();
    }

}
