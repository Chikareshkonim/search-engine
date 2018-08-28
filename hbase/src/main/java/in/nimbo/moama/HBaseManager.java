package in.nimbo.moama;

import com.google.protobuf.ServiceException;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.util.PropertyType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;

public class HBaseManager {
    TableName tableName;
    String family1;
    String family2;
    static Logger errorLogger = Logger.getLogger("error");
    Configuration configuration;
    static int sizeLimit = 0;
    private String checkColumn;
    final ArrayList<Put> puts;
    HBaseManager(String configPath) {
        configuration = HBaseConfiguration.create();
        configuration.addResource(getClass().getResourceAsStream("/hbase-site.xml"));
        tableName = TableName.valueOf(ConfigManager.getInstance().getProperty(PropertyType.H_BASE_TABLE));
        family1 = ConfigManager.getInstance().getProperty(PropertyType.H_BASE_CONTENT_FAMILY);
        family2 = ConfigManager.getInstance().getProperty(PropertyType.H_BASE_RANK_FAMILY);
        checkColumn = ConfigManager.getInstance().getProperty(PropertyType.H_BASE_COLUMN_PAGE_RANK);
        sizeLimit = Integer.parseInt(ConfigManager.getInstance().getProperty(PropertyType.PUT_SIZE_LIMIT));
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

    public boolean createTable() {
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Admin admin = connection.getAdmin();
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            hTableDescriptor.addFamily(new HColumnDescriptor(family1));
            hTableDescriptor.addFamily(new HColumnDescriptor(family2));
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

    String generateRowKeyFromUrl(String link) {
        String domain;
        try {
            domain = new URL(link).getHost();
        } catch (MalformedURLException e) {
            domain = "ERROR";
        }
        String[] urlSections = link.split(domain);
        String[] domainSections = domain.split("\\.");
        StringBuilder domainToHBase = new StringBuilder();
        for (int i = domainSections.length - 1; i >= 0; i--) {
            domainToHBase.append(domainSections[i]);
            if (i == 0) {
                if (!link.startsWith(domain)) {
                    domainToHBase.append(".").append(urlSections[0]);
                }
            } else {
                domainToHBase.append(".");
            }
        }
        return domainToHBase + "-" + urlSections[urlSections.length - 1];
    }
    public boolean checkDuplicate(String url) {
        Get get = new Get(Bytes.toBytes(generateRowKeyFromUrl(url)));
        get.addColumn(family2.getBytes(), checkColumn.getBytes());
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Table t = connection.getTable(tableName);
            Result result = t.get(get);
            if (result.listCells() == null) {
                return false;
            }
        } catch (IOException e) {
            errorLogger.error("HBase service unavailable");
        }
        return true;
    }
}
