package in.nimbo.moama.fetcher;

import in.nimbo.moama.RSSs;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RSSParser {
    public static List<NewsInfo> parse(String rss, String domain) throws IOException {
        System.out.println("pa rsing " + rss);
        List<NewsInfo> result = new ArrayList<>();
        Document document = Jsoup.connect(rss).validateTLSCertificates(false).get();
        for (Element element : document.getElementsByTag("item")) {
            if (!isSeen(element)) {
                String title = element.select("title").text();
                System.err.println(title);
                String date = element.select("pubDate").text();
                System.err.println(date);
                String url = element.select("link").text();
                System.err.println(url);
                NewsInfo newsInfo = new NewsInfo(title, date, url, domain);
                result.add(newsInfo);
            }
        }
        return result;
    }

    private static boolean isSeen(Element element) {
        return RSSs.getInstance().isSeen(element.getElementsByTag("link").get(0).text());
    }
}
