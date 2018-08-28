package in.nimbo.moama.rankcalculator;

import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.util.PropertyType;
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


public class ReferenceCalculator {
    private static Logger errorLogger = Logger.getLogger("error");
    private TableName webPageTable;
    private String contentFamily;
    private Configuration configuration;
    private static String refrencerank = "refrencerank";
    private static String refrenceFamilyName = "score";
    private Configuration hbaseConf;
    private JavaSparkContext sparkContext;

    public ReferenceCalculator(String appName, String master) {
        configuration = HBaseConfiguration.create();
        configuration.addResource(getClass().getResourceAsStream("/hbase-site.xml"));
        webPageTable = TableName.valueOf(ConfigManager.getInstance().getProperty(PropertyType.H_BASE_TABLE));
        contentFamily = ConfigManager.getInstance().getProperty(PropertyType.H_BASE_CONTENT_FAMILY);
        String[] jars = {"/home/rank/target/rank-1.0-SNAPSHOT-jar-with-dependencies.jar"};
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

    }
    private JavaPairRDD<String, Integer> getFromHBase() {
        JavaPairRDD<ImmutableBytesWritable, Result> data = sparkContext.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        return data.flatMapToPair(pair->{
            List<Cell> cells = pair._2.listCells();
            List<Tuple2<String,Integer>> resultList = new ArrayList<>();
            cells.forEach(cell -> resultList.add(new Tuple2<>(Bytes.toString(CellUtil.cloneQualifier(cell)),1)));
            return resultList.iterator();
        });
    }
    private void writeToHBase(JavaPairRDD<String, Integer> toWrite) {
        try {
            Job jobConfig = Job.getInstance(hbaseConf);
            // TODO: 8/11/18 replace test with webpage
            jobConfig.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, String.valueOf(webPageTable));
            jobConfig.setOutputFormatClass(TableOutputFormat.class);
            JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = toWrite.mapToPair(pair -> {
                Put put = new Put(Bytes.toBytes(pair._1));
                put.addColumn(refrenceFamilyName.getBytes(), refrencerank.getBytes(), Bytes.toBytes(pair._2));
                return new Tuple2<>(new ImmutableBytesWritable(), put);
            });
            hbasePuts.saveAsNewAPIHadoopDataset(jobConfig.getConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
