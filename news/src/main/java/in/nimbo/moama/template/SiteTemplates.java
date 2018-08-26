package in.nimbo.moama.template;

import java.util.LinkedHashMap;

public class SiteTemplates {
    private static SiteTemplates ourInstance;

    public static SiteTemplates getInstance() {
        return ourInstance;
    }

    private LinkedHashMap<String, Template> siteTemplates = new LinkedHashMap<>();

    public LinkedHashMap<String, Template> getSiteTemplates() {
        return siteTemplates;
    }

    public void saveTemplate() {
        // TODO: 8/18/18  
    }

    public void loadTemplates() {
        // TODO: 8/18/18  
    }
}