package in.nimbo.moama;

import com.google.protobuf.ServiceException;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.metrics.IntMeter;
import in.nimbo.moama.util.HBasePropertyType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Collectors;

public class HBaseManager {
    TableName tableName;
    String duplicateCheckFamily;
    protected static final Logger LOGGER = LogManager.getLogger(HBaseManager.class);
    Configuration configuration;
    Connection connection;
    private HTable table;
    private static final IntMeter HBASE_PUT_METER = new IntMeter("Hbase put");
    private static int hbaseBulkSize;
    static {
        hbaseBulkSize=ConfigManager.getInstance().getIntProperty(HBasePropertyType.HBASE_BULK_SIZE);
    }

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

    public static void exiting() {

    }
    LinkedList<Put> bulk=new LinkedList<>();
    public synchronized void puts(LinkedList<Put> unUseAblePut) {
            bulk.addAll(unUseAblePut);
            if (bulk.size()>hbaseBulkSize) {
                try {
                    table.put(bulk);
                    HBASE_PUT_METER.add(bulk.size());
                    bulk.clear();
                    ;
                } catch (IOException e) {
                    LOGGER.error("couldn't put  into HBase!", e);
                } catch (RuntimeException e) {
                    LOGGER.error("HBase error" + e.getMessage(), e);
                }
            }
    }

    public void put(List<Put> webDocOfThisThread) {
        try {
            table.put(webDocOfThisThread);
            HBASE_PUT_METER.add(webDocOfThisThread.size());
            webDocOfThisThread.clear();
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
    public boolean[] isDuplicate(List<String> urls) {
        try {
            return table.existsAll(urls.stream()
                    .map(url->new Get(Bytes.toBytes(generateRowKeyFromUrl(url))))
                    .peek(get -> get.addFamily(duplicateCheckFamily.getBytes()))
                    .collect(Collectors.toList()));
        } catch (IOException e) {
            LOGGER.error("HBase service unavailable");
        }
        return null;
    }

    public boolean isDuplicate(String url) {
        Get get = new Get(Bytes.toBytes(generateRowKeyFromUrl(url)));
        get.addFamily(duplicateCheckFamily.getBytes());
        try {
            return table.exists(get);
        } catch (IOException e) {
            LOGGER.error("HBase service unavailable");
        }
        return false;
    }

    public void close() {
        try {
            table.close();
            Utils.delay(1000);
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
