package in.nimbo.moama.fetcher;

import in.nimbo.moama.template.SiteTemplates;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;

class NewsParser {
    private static SiteTemplates siteTemplates = SiteTemplates.getInstance();

    static String parse(String domain, String url) throws IOException {
        Document document = Jsoup.connect(url).validateTLSCertificates(false).get();
        return document.getElementsByClass(siteTemplates.getSiteTemplates().get(domain).getAttributeValue()).text();
    }
}
