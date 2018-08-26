package in.nimbo.moama.database;

import in.nimbo.moama.util.ConfigManager;
import in.nimbo.moama.util.PropertyType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class HBaseDao {
    private TableName webPageTable = TableName.valueOf(ConfigManager.getInstance().getProperty(PropertyType.HBASE_TABLE2));
    private String contextFamily = ConfigManager.getInstance().getProperty(PropertyType.HBASE_FAMILY1);
    private Configuration configuration;
    Connection connection;

    public HBaseDao() {
        configuration = HBaseConfiguration.create();
        String path = this.getClass().getClassLoader().getResource("hbase-site.xml").getPath();
        configuration.addResource(new Path(path));
        boolean status = false;
        while(!status) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
                status = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public int getReference(String url){
        Get get = new Get(Bytes.toBytes(url));
        int score = 0;
        get.addColumn(contextFamily.getBytes(), "pageRank".getBytes());
        try{
            Table t = connection.getTable(webPageTable);
            Result result = t.get(get);
            if(result.listCells() != null) {
                List<Cell> cells =  result.listCells();
                score = Bytes.toInt(CellUtil.cloneValue(cells.get(0)));
            }
            else{
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
