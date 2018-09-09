package in.nimbo.moama.template;

import in.nimbo.moama.NewsWebsiteHBaseManager;
import in.nimbo.moama.configmanager.ConfigManager;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.LinkedHashMap;
import java.util.List;

import static in.nimbo.moama.newsutil.NewsPropertyType.*;

public class SiteTemplates {
    private static final Logger LOGGER = Logger.getLogger(SiteTemplates.class);
    private static SiteTemplates ourInstance = new SiteTemplates();


    public static SiteTemplates getInstance() {
        return ourInstance;
    }

    private SiteTemplates() {
        loadTemplates();
    }

    private LinkedHashMap<String, Template> siteTemplates = new LinkedHashMap<>();

    public LinkedHashMap<String, Template> getSiteTemplates() {
        return siteTemplates;
    }

    public Template getTemplate(String domain) {
        return siteTemplates.get(domain);
    }

    public void loadTemplates() {
        LOGGER.info("loading templates...");
        ConfigManager configManager = ConfigManager.getInstance();
        NewsWebsiteHBaseManager hBaseManager = new NewsWebsiteHBaseManager(configManager.getProperty(NEWS_WEBSITE_TABLE)
                , configManager.getProperty(HBASE_TEMPLATE_FAMILY), configManager.getProperty(HBASE_RSS_FAMILY));
        List<JSONObject> templates = hBaseManager.getTemplates();
        templates.forEach(json -> {
            String domain = json.getString("domain");
            Template template = new Template(json.getString("attModel"), json.getString("attValue"),
                    json.getString("dateFormat"), json.getString("newsTag"));
            siteTemplates.put(domain, template);
        });
        LOGGER.info("templates loaded successfully");
    }
}