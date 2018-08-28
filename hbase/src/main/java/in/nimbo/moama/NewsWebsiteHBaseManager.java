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
    public NewsWebsiteHBaseManager(String tableName) {
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

    }
}
