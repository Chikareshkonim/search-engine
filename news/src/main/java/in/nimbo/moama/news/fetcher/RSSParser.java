package in.nimbo.moama.news.fetcher;

import in.nimbo.moama.news.RSSs;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.news.template.SiteTemplates;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import static in.nimbo.moama.news.newsutil.NewsPropertyType.NEWS_DATE_FORMAT;

public class RSSParser {
    private static String UNIQUE_DATE_FORMAT = ConfigManager.getInstance().getProperty(NEWS_DATE_FORMAT);
    private static final Logger LOGGER = Logger.getLogger(RSSParser.class);


    public static List<NewsInfo> parse(String rss, String domain) throws IOException {
        List<NewsInfo> result = new ArrayList<>();
        Document document = Jsoup.connect(rss).validateTLSCertificates(false).get();
        for (Element element : document.getElementsByTag("item")) {
            if (!isSeen(element)) {
                String title = element.select("title").text();
                String date = null;
                try {
                    date = uniqueDateFormat(SiteTemplates.getInstance().getTemplate(domain).getDateFormatString(),
                            element.select("pubDate").text());
                } catch (ParseException e) {
                    LOGGER.error("ParseException at RSSParser", e);
                }
                String url = element.select("link").text();
                NewsInfo newsInfo = new NewsInfo(title, date, url, domain);
                result.add(newsInfo);
                LOGGER.trace("Added to queue: " + newsInfo);
            }
        }
        return result;
    }

    static String uniqueDateFormat(String dateFormatString, String date) throws ParseException {
        SimpleDateFormat newsFormat = new SimpleDateFormat(dateFormatString);
        SimpleDateFormat uniqueFormat = new SimpleDateFormat(UNIQUE_DATE_FORMAT);
        return uniqueFormat.format(newsFormat.parse(date));
    }

    private static boolean isSeen(Element element) {
        return RSSs.getInstance().isSeen(element.getElementsByTag("link").get(0).text());
    }
}
