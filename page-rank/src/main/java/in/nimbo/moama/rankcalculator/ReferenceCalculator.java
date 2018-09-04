package in.nimbo.moama.rankcalculator;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//TODO: set config file
public class ReferenceCalculator {
    private static final Logger logger = Logger.getLogger(ReferenceCalculator.class);
    private TableName webPageTable;
    private String contentFamily;
    private static String refrenceColumn;
    private static String refrenceFamilyName;
    private Configuration hbaseConf;
    private JavaSparkContext sparkContext;

    public ReferenceCalculator(String appName, String master) {
        webPageTable = TableName.valueOf("pages");
        contentFamily = "outLinks";
        refrenceFamilyName = "score";
        refrenceColumn = "reference";
        String[] jars = {"/home/moama/search-engine/page-rank/target/page-rank-1.0-SNAPSHOT-jar-with-dependencies.jar"};
        SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster(master).setJars(jars);
        sparkContext = new JavaSparkContext(sparkConf);
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.addResource(getClass().getResource("/hbase-site.xml"));
        hbaseConf.addResource(getClass().getResource("/core-site.xml"));
        hbaseConf.set(TableInputFormat.INPUT_TABLE, String.valueOf(webPageTable));
        hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, contentFamily);
    }
    public void calculate(){
        JavaPairRDD<String,Integer> input = getFromHBase();
        JavaPairRDD<String,Integer> result = input.reduceByKey((value1,value2)-> value1+value2);
        writeToHBase(result);

    }
    private JavaPairRDD<String, Integer> getFromHBase() {
        JavaPairRDD<ImmutableBytesWritable, Result> data = sparkContext.newAPIHadoopRDD(hbaseConf, TableInputFormat.class,
                ImmutableBytesWritable.class, Result.class);
        return data.flatMapToPair(pair->{
            List<Tuple2<String,Integer>> resultList = new ArrayList<>();
            List<Cell> cells = pair._2.listCells();
            cells.forEach(cell -> resultList.add(new Tuple2<>(Bytes.toString(CellUtil.cloneQualifier(cell)),1)));
            return resultList.iterator();
        });
    }
    private void writeToHBase(JavaPairRDD<String, Integer> toWrite) {
        try {
            Job jobConfig = Job.getInstance(hbaseConf);
            jobConfig.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, String.valueOf(webPageTable));
            jobConfig.setOutputFormatClass(TableOutputFormat.class);
            JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = toWrite.mapToPair(pair -> {
                try {
                    Put put = new Put(Bytes.toBytes(pair._1));
                    put.addColumn(refrenceFamilyName.getBytes(), refrenceColumn.getBytes(), Bytes.toBytes(pair._2));
                    return new Tuple2<>(new ImmutableBytesWritable(), put);
                }catch (Exception e){
                    logger.error(e.getMessage());
                }
                return null;
            });
            hbasePuts.saveAsNewAPIHadoopDataset(jobConfig.getConfiguration());
        } catch (Exception e) {
            //TODO : set logger
            logger.error(e.getMessage());
        }
    }
}
