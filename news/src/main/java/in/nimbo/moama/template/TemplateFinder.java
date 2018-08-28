package in.nimbo.moama.template;

import in.nimbo.moama.Util;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class TemplateFinder {
    public static void main(String[] args) throws IOException {
        System.out.println(findTemplate("http://www.asriran.com/fa/rss/1", "link"));
    }

    public static Template findTemplate(String rss, String newsTag) throws IOException {
        Document rssDoc = Jsoup.connect(rss).get();
        Document goodPage = findMaxTextPage(rssDoc, newsTag);
        System.out.println(goodPage.text());
        String dateFormat = findDateFormat(rssDoc);
        System.out.println(dateFormat);
        String newsTextAddress = findTextAddress(goodPage);
        System.out.println(newsTextAddress);
        return new Template("Class", newsTextAddress, dateFormat, newsTag);
    }

    private static String findTextAddress(Document goodPage) {
        List<MyElement> myElements = goodPage.getElementsByAttribute("class").stream()
                .filter(classElement -> !classElement.outerHtml().contains("mobile"))
                .map(MyElement::new).collect(Collectors.toList());
        Collections.reverse(myElements);
        myElements.sort(Collections.reverseOrder());
        for (int i = 0; i < myElements.size(); i++) {
            if (!myElements.get(i).getElement().text().contains(myElements.get(i + 1).getElement().text())) {
                return myElements.get(i).getElement().className();
            }
        }
        return null;
    }

    private static String findDateFormat(Document rssDoc) {
        return rssDoc.getElementsByTag("pubDate").stream().findFirst()
                .map(Element::text).map(DateFormatFinder::parse).get();
    }

    private static Document findMaxTextPage(Document rssDoc, String newsTag) {
        Stream<Element> stream = rssDoc.getElementsByTag("item").stream()
                .flatMap(element -> element.getElementsByTag(newsTag).stream()).skip(4).limit(25);
        return stream.parallel().peek(element -> System.out.println(element.html())
            ).map(Element::text).map(Util::getPage).max(Comparator.comparing(a -> a.text().length())).get();
    }
}

class MyElement implements Comparable<MyElement> {
    private int numberOfDots;
    private Element element;

    MyElement(Element element) {
        this.element = element;
        String string = element.text().replaceAll("[.]+", ".");
        numberOfDots = string.length() - string.replaceAll("[.]+", "").length();
    }

    @Override
    public int compareTo(MyElement o) {
        return numberOfDots - o.numberOfDots;
    }

    Element getElement() {
        return element;
    }
}

class DateFormatFinder {

    private static final String[] formats =
            {
                    "EEE, dd MMM yyyy HH:mm:ss Z",
                    "dd MMM yyyy HH:mm:ss Z",
                    "EEE, dd MMM yyyy HH:mm",
                    "dd MMM yyyy HH:mm",
                    "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
                    "EEE, d MMM yyyy HH:mm:ss Z",
                    "EEE, dd MMM yyyy HH:mm:ss zzz",
                    "yyyy-mm-dd HH:mm:ss",
                    "yyyy-mm-dd hh:mm:ss",
                    "yyyy-MM-dd'T'HH:mm:ssZ",
                    "yyyy-MM-dd'T'HH:mm:ss",
                    "yyyy-MM-dd'T'HH:mm:ssZ",
                    "yyyy-MM-dd'T'HH:mm:ss Z",
                    "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
                    "yyyy-MM-dd'T'hh:mm:ssXXX",
                    "dd MMM yyyy HH:mm:ss Z",
                    "MM/dd/yyyy",
            };

    static String parse(String date) {
        return Stream.of(formats).filter(format -> {
            try {
                new SimpleDateFormat(format).parse(date);
                return true;
            } catch (ParseException e) {
                return false;
            }
        }).findFirst().get();
    }
}