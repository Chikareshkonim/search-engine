package in.nimbo.moama;

import com.google.protobuf.ServiceException;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.metrics.Metrics;
import in.nimbo.moama.util.PropertyType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static in.nimbo.moama.configmanager.ConfigManager.FileType.PROPERTIES;

public class WebDocumentHBaseManager extends HBaseManager{
    private static Logger errorLogger = Logger.getLogger("error");
    private static int size = 0;
    private static int added = 0;

    public WebDocumentHBaseManager(String configPath) {
        super(configPath);

    }

    public void put(JSONObject document) {
        String pageRankColumn = configManager.getProperty(PropertyType.H_BASE_COLUMN_PAGE_RANK);
        Put put = new Put(Bytes.toBytes(generateRowKeyFromUrl((String) document.get("pageLink"))));
        for (Object link : (JSONArray)document.get("outLinks")) {
            put.addColumn(family1.getBytes(), generateRowKeyFromUrl(((String)((JSONObject)link).get("LinkUrl"))).getBytes(), ((String)((JSONObject)link).get("LinkAnchor")).getBytes());
        }
        put.addColumn(family2.getBytes(), pageRankColumn.getBytes(), Bytes.toBytes(1.0));
        puts.add(put);
        size++;
        if (size >= sizeLimit) {
            synchronized (puts) {
                try (Connection connection = ConnectionFactory.createConnection(configuration)) {
                    Table t = connection.getTable(tableName);
                    t.put(puts);
                    t.close();
                    puts.clear();
                    added += size;
                    Metrics.numberOfPagesAddedToHBase = added;
                    size = 0;
                } catch (IOException e) {
                    //TODO
                    errorLogger.error("couldn't put document for " + document.get("pageLink") + " into HBase!");
                } catch (RuntimeException e) {
                    //TODO
                    errorLogger.error("HBase error" + e.getMessage());
                }
            }
        }
    }

    public int getReference(String url) {
        Get get = new Get(Bytes.toBytes(url));
        int score = 0;
        get.addColumn(family1.getBytes(), family2.getBytes());
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Table t = connection.getTable(tableName);
            Result result = t.get(get);
            if (result.listCells() != null) {
                List<Cell> cells = result.listCells();
                score = Bytes.toInt(CellUtil.cloneValue(cells.get(0)));
            } else {
                System.out.println("url not found in HBase! Page Reference set to 1 on default!");
                score = 1;
            }
            System.out.println("Page Reference for " + url + " is: " + score);

        } catch (IOException e) {
            System.out.println("couldn't get document for " + url + " from HBase!");
        }
        return score;
    }

}
