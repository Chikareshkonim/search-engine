package in.nimbo.moama;

import com.google.protobuf.ServiceException;
import in.nimbo.moama.configmanager.ConfigManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.IOException;

public class NewsHBaseManager extends HBaseManager {
    private ConfigManager configManager;
    private static Logger errorLogger = Logger.getLogger("error");
    private String twitterFamily;
    private String visitedFamily;
    private Configuration configuration;
    private static int sizeLimit = 0;
    private static int size = 0;

    public NewsHBaseManager(String tableName, String twitterFamily, String visitedFamily) {
        super(tableName, visitedFamily);
        boolean status = false;
        while (!status) {
            try {
                HBaseAdmin.checkHBaseAvailable(configuration);
                status = true;
            } catch (ServiceException | IOException e) {
                errorLogger.error(e.getMessage());
            }
        }
    }

    public void put(JSONObject document) {
        String url = (String) document.get("url");
        Put put = new Put(Bytes.toBytes(generateRowKeyFromUrl(url)));
        put.addColumn(duplicateCheckFamily.getBytes(), "visited".getBytes(), new byte[0]);
        puts.add(put);
        size++;
        if (size >= sizeLimit) {
            try(Connection connection = ConnectionFactory.createConnection(configuration)) {
                Table table = connection.getTable(tableName);
                table.put(puts);
                puts.clear();
                table.close();
                size = 0;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
