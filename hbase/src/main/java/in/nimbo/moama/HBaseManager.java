package in.nimbo.moama;

import com.google.protobuf.ServiceException;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.document.Link;
import in.nimbo.moama.document.WebDocument;
import in.nimbo.moama.metrics.Metrics;
import in.nimbo.moama.util.PropertyType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static in.nimbo.moama.configmanager.ConfigManager.FileType.PROPERTIES;

public class HBaseManager {
    private ConfigManager configManager;
    private static Logger errorLogger = Logger.getLogger("error");
    private TableName webPageTable;
    private String contentFamily;
    private String rankFamily;
    private String rankColumn;
    private Configuration configuration;
    private final List<Put> puts;
    private static int size = 0;
    private static int sizeLimit = 0;
    private static int added = 0;

    public HBaseManager() {
        try {
            configManager = new ConfigManager(new File(getClass().getClassLoader().getResource("config.properties").getFile()).getAbsolutePath(), PROPERTIES);
        } catch (IOException e) {
            errorLogger.error("Loading properties failed");
        }
        configuration = HBaseConfiguration.create();
        configuration.addResource(getClass().getResourceAsStream("/hbase-site.xml"));
        webPageTable = TableName.valueOf(configManager.getProperty(PropertyType.H_BASE_TABLE));
        contentFamily = configManager.getProperty(PropertyType.H_BASE_CONTENT_FAMILY);
        rankFamily = configManager.getProperty(PropertyType.H_BASE_RANK_FAMILY);
        rankColumn = configManager.getProperty(PropertyType.H_BASE_COLUMN_PAGE_RANK);
        sizeLimit = Integer.parseInt(configManager.getProperty(PropertyType.PUT_SIZE_LIMIT));
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
            HTableDescriptor hTableDescriptor = new HTableDescriptor(webPageTable);
            hTableDescriptor.addFamily(new HColumnDescriptor(contentFamily));
            hTableDescriptor.addFamily(new HColumnDescriptor(rankFamily));
            if (!admin.tableExists(webPageTable))
                admin.createTable(hTableDescriptor);
            admin.close();
            connection.close();
            return true;

        } catch (IOException e) {
            errorLogger.error(e.getMessage());
            return false;
        }
    }

    public void put(WebDocument document) {
        String pageRankColumn = configManager.getProperty(PropertyType.H_BASE_COLUMN_PAGE_RANK);
        Put put = new Put(Bytes.toBytes(generateRowKeyFromUrl(document.getPageLink())));
        for (Link link : document.getLinks()) {
            put.addColumn(contentFamily.getBytes(), link.getUrl().getBytes(), link.getAnchorLink().getBytes());
        }
        put.addColumn(rankFamily.getBytes(), pageRankColumn.getBytes(), Bytes.toBytes(1.0));
        puts.add(put);
        size++;
        if (size >= sizeLimit) {
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
                    //TODO
                    errorLogger.error("couldn't put document for " + document.getPageLink() + " into HBase!");
                } catch (RuntimeException e) {
                    //TODO
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

    public int getReference(String url) {
        Get get = new Get(Bytes.toBytes(url));
        int score = 0;
        get.addColumn(contentFamily.getBytes(), rankFamily.getBytes());
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Table t = connection.getTable(webPageTable);
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
    public boolean checkDuplicate(String url) {
        Get get = new Get(Bytes.toBytes(generateRowKeyFromUrl(url)));
        get.addColumn(rankFamily.getBytes(), rankColumn.getBytes());
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Table t = connection.getTable(webPageTable);
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
