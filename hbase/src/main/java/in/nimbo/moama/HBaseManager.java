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
    String duplicateCheckFamily;
    static Logger errorLogger = Logger.getLogger(HBaseManager.class);
    Configuration configuration;
    static int sizeLimit = 0;
    final ArrayList<Put> puts;
    Connection connection;

    public HBaseManager(String tableName, String duplicateCheckFamily) {
        configuration = HBaseConfiguration.create();
        configuration.addResource(getClass().getResourceAsStream("/hbase-site.xml"));
        this.tableName = TableName.valueOf(tableName);
        this.duplicateCheckFamily = duplicateCheckFamily;
        sizeLimit = Integer.parseInt(ConfigManager.getInstance().getProperty(HBasePropertyType.PUT_SIZE_LIMIT));
        puts = new ArrayList<>();
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        String string = "";
        try {
            string = domainToHBase + "-" + urlSections[urlSections.length - 1];
        }catch (Exception e){
            System.out.println(link);
            e.printStackTrace();
            System.out.println();
            System.out.println(e.getMessage());
        }
        return string;
    }

    public boolean isDuplicate(String url) {
        Get get = new Get(Bytes.toBytes(generateRowKeyFromUrl(url)));
        get.addFamily(duplicateCheckFamily.getBytes());
        Table t = null;
        try  {
            t = connection.getTable(tableName);
            if (t.exists(get)) {
                t.close();
                return true;
            }
        } catch (IOException e) {
            errorLogger.error("HBase service unavailable");
        }finally {
            try {
                assert t != null;
                t.close();
            } catch (IOException e) {
                errorLogger.error("table is not open");
            }
        }
        return false;
    }
}
