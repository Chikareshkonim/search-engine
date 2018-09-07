package in.nimbo.moama;

import com.google.protobuf.ServiceException;
import in.nimbo.moama.metrics.IntMeter;
import in.nimbo.moama.metrics.JMXManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

public class HBaseManager {
    TableName tableName;
    String duplicateCheckFamily;
    private static final Logger LOGGER = Logger.getLogger(HBaseManager.class);
    Configuration configuration;
    Connection connection;
    private JMXManager jmxManager = JMXManager.getInstance();
    private static IntMeter numberOfPagesAddedToHBase = new IntMeter("Hbase Added       ");
    private HTable table;

    public HBaseManager(String tableName, String duplicateCheckFamily) {
        configuration = HBaseConfiguration.create();
        configuration.addResource(getClass().getResourceAsStream("/hbase-site.xml"));
        this.tableName = TableName.valueOf(tableName);
        this.duplicateCheckFamily = duplicateCheckFamily;
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            //TODO
            e.printStackTrace();
        }
        boolean status = false;
        while (!status) {
            try {
                HBaseAdmin.checkHBaseAvailable(configuration);
                status = true;
            } catch (ServiceException | IOException e) {
                LOGGER.error(e.getMessage());
            }
        }
        try {
            table = (HTable) connection.getTable(this.tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void put(List<Put> webDocOfThisThread) {
        try {
            table.put(webDocOfThisThread);
            numberOfPagesAddedToHBase.add(webDocOfThisThread.size());
            webDocOfThisThread.clear();
            jmxManager.markNewAddedToHBase(webDocOfThisThread.size());
        } catch (IOException e) {
            LOGGER.error("couldn't put  into HBase!", e);
        } catch (RuntimeException e) {
            LOGGER.error("HBase error" + e.getMessage(), e);
        }
    }

    public String generateRowKeyFromUrl(String link) {
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
        } catch (Exception e) {
            //TODO
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
        try {
            if (table.exists(get)) {
                return true;
            }
        } catch (IOException e) {
            LOGGER.error("HBase service unavailable");
        }
        return false;
    }
}
