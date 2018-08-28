package in.nimbo.moama;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NewsWebsiteHBaseManager extends HBaseManager{
    private final List<Put> puts;
    private String templateFamily;
    private String rssFamily;
    public NewsWebsiteHBaseManager(String tableName, String templateFamily, String rssFamily) {
        super(tableName, templateFamily);
        this.templateFamily = templateFamily;
        this.rssFamily = rssFamily;
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

    }
}
