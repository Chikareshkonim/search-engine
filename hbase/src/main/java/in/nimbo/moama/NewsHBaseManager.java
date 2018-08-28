package in.nimbo.moama;

import com.google.protobuf.ServiceException;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.document.Link;
import in.nimbo.moama.document.WebDocument;
import in.nimbo.moama.metrics.Metrics;
import in.nimbo.moama.util.PropertyType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static in.nimbo.moama.configmanager.ConfigManager.FileType.PROPERTIES;

public class NewsHBaseManager extends HBaseManager {
    private ConfigManager configManager;
    private static Logger errorLogger = Logger.getLogger("error");
    private TableName newsPageTable;
    private String tweetsFamily;
    private Configuration configuration;
    private final List<Put> puts;
    private static int sizeLimit = 0;
    private static int size = 0;

    public NewsHBaseManager(String configPath) {
        super(configPath);
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
