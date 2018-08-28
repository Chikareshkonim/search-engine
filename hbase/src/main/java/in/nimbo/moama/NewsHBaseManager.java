package in.nimbo.moama;

import com.google.protobuf.ServiceException;
import in.nimbo.moama.configmanager.ConfigManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NewsHBaseManager extends HBaseManager {
    private ConfigManager configManager;
    private static Logger errorLogger = Logger.getLogger("error");
    private TableName newsPageTable;
    private String tweetsFamily;
    private Configuration configuration;
    private final List<Put> puts;
    private static int sizeLimit = 0;
    private static int size = 0;

    public NewsHBaseManager(String tableName) {
        super(tableName);
        puts = new ArrayList<>();
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
        //TODO
    }
}
