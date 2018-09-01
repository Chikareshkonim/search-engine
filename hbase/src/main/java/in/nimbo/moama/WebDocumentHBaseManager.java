package in.nimbo.moama;

import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.metrics.JMXManager;
import in.nimbo.moama.metrics.Metrics;
import in.nimbo.moama.util.HBasePropertyType;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.List;

public class WebDocumentHBaseManager extends HBaseManager{
    private static Logger errorLogger = Logger.getLogger("error");
    private String outLinksFamily;
    private String scoreFamily;
    private static int size = 0;
    private static int added = 0;

    public WebDocumentHBaseManager(String tableName, String outLinksFamily, String scoreFamily)  {
        super(tableName, scoreFamily);
        this.outLinksFamily = outLinksFamily;
        this.scoreFamily = scoreFamily;
    }

    public void put(JSONObject document, JMXManager jmxManager) {
        String pageRankColumn = ConfigManager.getInstance().getProperty(HBasePropertyType.HBASE_DUPCHECK_COLUMN);
        Put put = new Put(Bytes.toBytes(generateRowKeyFromUrl((String) document.get("pageLink"))));
        for (Object link : (JSONArray)document.get("outLinks")) {
            put.addColumn(outLinksFamily.getBytes(), generateRowKeyFromUrl(((String)((JSONObject)link).get("LinkUrl"))).getBytes(), ((String)((JSONObject)link).get("LinkAnchor")).getBytes());
        }
        put.addColumn(scoreFamily.getBytes(), pageRankColumn.getBytes(), Bytes.toBytes(1.0));
        puts.add(put);
        if (size >= sizeLimit) {
            synchronized (puts) {
                try  {
                    Table t = connection.getTable(tableName);
                    t.put(puts);
                    errorLogger.error("added");
                    t.close();
                    added += puts.size();
                    System.out.println("habse out size"+puts.size());
                    puts.clear();
                    Metrics.numberOfPagesAddedToHBase = added;
                    jmxManager.markNewAddedToHBase();
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
        get.addColumn(outLinksFamily.getBytes(), scoreFamily.getBytes());
        try  {
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

    public boolean createTable() {
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Admin admin = connection.getAdmin();
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            hTableDescriptor.addFamily(new HColumnDescriptor(outLinksFamily));
            hTableDescriptor.addFamily(new HColumnDescriptor(scoreFamily));
            if (!admin.tableExists(tableName))
                admin.createTable(hTableDescriptor);
            admin.close();
            connection.close();
            return true;
        } catch (IOException e) {
            errorLogger.error(e.getMessage());
            return false;
        }
    }
}
