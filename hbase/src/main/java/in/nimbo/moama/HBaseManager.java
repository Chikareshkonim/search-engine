package in.nimbo.moama;

import in.nimbo.moama.ConfigManager;
import in.nimbo.moama.document.WebDocument;
import in.nimbo.moama.metrics.Metrics;
import in.nimbo.moama.util.PropertyType;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class HBaseManager {
    private ConfigManager configManager;
    private static Logger errorLogger = Logger.getLogger("error");
    private TableName webPageTable = TableName.valueOf(configManager.getProperty(PropertyType.H_BASE_TABLE));
    private String contextFamily = configManager.getProperty(PropertyType.H_BASE_FAMILY_1);
    private String rankFamily = configManager.getProperty(PropertyType.H_BASE_FAMILY_2);
    private Configuration configuration;
    private final List<Put> puts;
    private static int size = 0;
    private final static int SIZE_LIMIT = 100;
    private static int added = 0;

    public HBaseManager() {
        configuration = HBaseConfiguration.create();
        configuration.addResource(getClass().getResourceAsStream("/hbase-site.xml"));
        puts = new ArrayList<>();
        boolean status = false;
        while (!status) {
            try {
                HBaseAdmin.available(configuration);
                status = true;
            } catch (IOException e) {
                errorLogger.error(e.getMessage());
            }
        }
    }

    public boolean createTable() {
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Admin admin = connection.getAdmin();
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(webPageTable);
            ColumnFamilyDescriptorBuilder anchorFamilyBuilderContext = ColumnFamilyDescriptorBuilder
                    .newBuilder(contextFamily.getBytes());
            ColumnFamilyDescriptorBuilder anchorFamilyBuilderRank = ColumnFamilyDescriptorBuilder
                    .newBuilder(rankFamily.getBytes());
            tableDescriptorBuilder.setColumnFamily(anchorFamilyBuilderContext.build());
            tableDescriptorBuilder.setColumnFamily(anchorFamilyBuilderRank.build());
            if (!admin.tableExists(webPageTable))
                admin.createTable(tableDescriptorBuilder.build());
            System.out.println("create");
            admin.close();
            connection.close();
            return true;

        } catch (IOException e) {
            errorLogger.error(e.getMessage());
            return false;
        }
    }

    public void put(WebDocument document) {
        String outLinksColumn = configManager.getProperty(PropertyType.H_BASE_COLUMN_OUT_LINKS);
        String pageRankColumn = configManager.getProperty(PropertyType.H_BASE_COLUMN_PAGE_RANK);
        Put put = new Put(Bytes.toBytes(generateRowKeyFromUrl(document.getPagelink())));
        byte[] outLinks = SerializationUtils.serialize(document.getLinks());
        put.addColumn(contextFamily.getBytes(), outLinksColumn.getBytes(), outLinks);
        put.addColumn(rankFamily.getBytes(), pageRankColumn.getBytes(), Bytes.toBytes(1.0));
        puts.add(put);
        size++;
        if (size >= SIZE_LIMIT) {
            synchronized (puts) {
                try (Connection connection = ConnectionFactory.createConnection(configuration)) {
                    Table t = connection.getTable(webPageTable);
                    t.put(puts);
                    t.close();
                    puts.clear();
                    added += size;
                    Metrics.numberOfPagesAddedToHBase = added;
                    size = 0;
                } catch (IOException e) {
                    errorLogger.error("couldn't put document for " + document.getPagelink() + " into HBase!");
                } catch (RuntimeException e) {
                    errorLogger.error("HBase error" + e.getMessage());
                }
            }
        }
    }

    private String generateRowKeyFromUrl(String url) {
        String domain;
        try {
            domain = new URL(url).getHost();
        } catch (MalformedURLException e) {
            domain = "ERROR";
        }
        String[] urlSections = url.split(domain);
        String[] domainSections = domain.split("\\.");
        StringBuilder domainToHBase = new StringBuilder();
        for (int i = domainSections.length - 1; i >= 0; i--) {
            domainToHBase.append(domainSections[i]);
            if (i == 0) {
                if (!url.startsWith(domain)) {
                    domainToHBase.append(".").append(urlSections[0]);
                }
            } else {
                domainToHBase.append(".");
            }
        }
        return domainToHBase + "-" + urlSections[urlSections.length - 1];
    }

    public int getReference(String url){
        Get get = new Get(Bytes.toBytes(url));
        int score = 0;
        get.addColumn(contextFamily.getBytes(), "pageRank".getBytes());
        try(Connection connection = ConnectionFactory.createConnection(configuration)){
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
