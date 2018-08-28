package in.nimbo.moama;

import com.google.protobuf.ServiceException;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.util.HBasePropertyType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
    private String duplicateCheckFamily;
    static Logger errorLogger = Logger.getLogger("error");
    Configuration configuration;
    static int sizeLimit = 0;
    private String checkColumn;
    final ArrayList<Put> puts;

    HBaseManager(String tableName, String duplicateCheckFamily) {
        configuration = HBaseConfiguration.create();
        configuration.addResource(getClass().getResourceAsStream("/hbase-site.xml"));
        this.tableName = TableName.valueOf(tableName);
        this.duplicateCheckFamily = duplicateCheckFamily;
        checkColumn = ConfigManager.getInstance().getProperty(HBasePropertyType.HBASE_COLUMN_PAGE_RANK);
        sizeLimit = Integer.parseInt(ConfigManager.getInstance().getProperty(HBasePropertyType.PUT_SIZE_LIMIT));
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
        get.addColumn(duplicateCheckFamily.getBytes(), checkColumn.getBytes());
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
