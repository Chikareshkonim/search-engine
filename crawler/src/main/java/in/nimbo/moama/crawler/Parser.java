package in.nimbo.moama.crawler;

import in.nimbo.moama.crawler.domainvalidation.DuplicateHandler;
import in.nimbo.moama.document.Link;
import in.nimbo.moama.UrlHandler;
import in.nimbo.moama.document.WebDocument;
import in.nimbo.moama.crawler.language.LangDetector;
import in.nimbo.moama.exception.IllegalLanguageException;
import in.nimbo.moama.metrics.IntMeter;
import in.nimbo.moama.metrics.JMXManager;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;

public class Parser {
    private static LangDetector langDetector;
    private static Parser ourInstance=new Parser();
    private static DuplicateHandler duplicateChecker=DuplicateHandler.getInstance();
    private static JMXManager jmxManager=JMXManager.getInstance();

    private static IntMeter crawledPage=new IntMeter("crawled Page");
    private static IntMeter languagePassed=new IntMeter("language passed");
    private static IntMeter urlReceived=new IntMeter("url received");

    public synchronized static Parser getInstance() {
        return ourInstance;
    }

    private Parser() {
    }

    public static void setLangDetector(LangDetector langDetector) {
        Parser.langDetector = langDetector;
    }

    public WebDocument parse(String url) throws IllegalLanguageException, IOException {
        Document document = Jsoup.connect(url).validateTLSCertificates(false).get();
        duplicateChecker.weakConfirm(url);
        urlReceived.increment();
//        jmxManager.markNewUrlReceived();

        WebDocument webDocument = new WebDocument();
        String text = document.text();
        checkLanguage(document, text);
        languagePassed.increment();
//        jmxManager.markNewLanguagePassed();

        Link[] links = UrlHandler.getLinks(document.getElementsByTag("a"), new URL(url).getHost());
        webDocument.setTextDoc(text);
        webDocument.setTitle(document.title());
        webDocument.setPageLink(url);
        webDocument.setLinks(new ArrayList<>(Arrays.asList(links)));
        crawledPage.increment();
//        jmxManager.markNewCrawledPage();

        return webDocument;
    }

    private void checkLanguage(Document document, String text) throws IllegalLanguageException {
//        try {
//            String lang = document.getElementsByAttribute("lang").get(0).attr("lang").toLowerCase();
//            if (lang.equals("en") || lang.startsWith("en-")) {
//                return;
//            }
//            throw new IllegalLanguageException();
//        } catch (RuntimeException e) {
//            //getElementsByAttribute throws a NullPointerException if document doesn't have lang tag
            langDetector.languageCheckHard(text);
//        }
    }
}

