package in.nimbo.moama;

import com.google.gson.Gson;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
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

    }

    public boolean createTable(List<JSONObject> jsons) {
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Admin admin = connection.getAdmin();
            if (!admin.tableExists(tableName)) {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                hTableDescriptor.addFamily(new HColumnDescriptor(templateFamily));
                hTableDescriptor.addFamily(new HColumnDescriptor(rssFamily));
                admin.createTable(hTableDescriptor);
                List<Put> puts = new ArrayList<>();
                // TODO: 8/29/18 Change
                for (JSONObject json : jsons) {
                    Put put = new Put(json.getString("domain").getBytes());
                    put.addColumn(templateFamily.getBytes(), "attModel".getBytes(), json.getString("attModel").getBytes());
                    put.addColumn(templateFamily.getBytes(), "attValue".getBytes(), json.getString("attValue").getBytes());
                    put.addColumn(templateFamily.getBytes(), "dateFormat".getBytes(), json.getString("dateFormat").getBytes());
                    put.addColumn(templateFamily.getBytes(), "newsTag".getBytes(), json.getString("newsTag").getBytes());
                    List<String> rss = new Gson().fromJson(json.getString("rss"), List.class);
                    for (int i = 0; i < rss.size(); i++) {
                        put.addColumn(rssFamily.getBytes(), ("" + i).getBytes(), rss.get(i).getBytes());
                    }
                    puts.add(put);
                }
                Table table = connection.getTable(tableName);
                table.put(puts);
                table.close();
                return true;
            }
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    public List<JSONObject> getTemplates() {
        List<JSONObject> jsons = new ArrayList<>();
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Table table = connection.getTable(tableName);
            Scan scan = new Scan();
            scan.setMaxVersions(1);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                JSONObject json = new JSONObject();
                json.put("attModel", Bytes.toString(result.getValue(templateFamily.getBytes(), "attModel".getBytes())));
                json.put("attValue", Bytes.toString(result.getValue(templateFamily.getBytes(), "attValue".getBytes())));
                json.put("dateFormat", Bytes.toString(result.getValue(templateFamily.getBytes(), "dateFormat".getBytes())));
                json.put("newsTag", Bytes.toString(result.getValue(templateFamily.getBytes(), "newsTag".getBytes())));
                json.put("domain", Bytes.toString(result.getRow()));
                jsons.add(json);
            }

        } catch (IOException ignored) {
        }
        return jsons;
    }

    public List<JSONObject> getRSSList() {
        List<JSONObject> jsons = new ArrayList<>();
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Table table = connection.getTable(tableName);
            Scan scan = new Scan();
            scan.setMaxVersions(1);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                result.getFamilyMap(rssFamily.getBytes()).forEach((key, value) -> {
                    JSONObject json = new JSONObject();
                    json.put("rss", Bytes.toString(value));
                    json.put("domain", Bytes.toString(result.getRow()));
                    jsons.add(json);
                });
            }
        } catch (IOException ignored) {
        }
        return jsons;
    }
}
