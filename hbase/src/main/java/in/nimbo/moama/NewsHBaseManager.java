package in.nimbo.moama;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.IOException;

public class NewsHBaseManager extends HBaseManager {
    private static Logger errorLogger = Logger.getLogger("error");
    private String twitterFamily;
    private String visitedFamily;
    private static int sizeLimit = 0;
    private static int size = 0;

    public NewsHBaseManager(String tableName, String twitterFamily, String visitedFamily) {
        super(tableName, visitedFamily);
        this.twitterFamily = twitterFamily;
        this.visitedFamily = visitedFamily;
    }


}
