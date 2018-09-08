package in.nimbo.moama.crawler;

import in.nimbo.moama.crawler.domainvalidation.DuplicateHandler;
import in.nimbo.moama.document.Link;
import in.nimbo.moama.UrlHandler;
import in.nimbo.moama.document.WebDocument;
import in.nimbo.moama.crawler.language.LangDetector;
import in.nimbo.moama.exception.IllegalLanguageException;
import in.nimbo.moama.metrics.FloatMeter;
import in.nimbo.moama.metrics.IntMeter;
import in.nimbo.moama.metrics.JMXManager;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;


public class Parser {
    private static LangDetector langDetector;
    private static Parser ourInstance=new Parser();
    private static JMXManager jmxManager=JMXManager.getInstance();

    private static IntMeter crawledPage=new IntMeter("crawled Page");
    private static IntMeter languagePassed=new IntMeter("language passed");
    private static FloatMeter langDetectTime   = new FloatMeter("lang detect Time") ;
    private static FloatMeter webdocCreateTime = new FloatMeter("webdoc create Time");

    public synchronized static Parser getInstance() {
        return ourInstance;
    }

    private Parser() {
    }

    public static void setLangDetector(LangDetector langDetector) {
        Parser.langDetector = langDetector;
    }

    public WebDocument parse(Document document ,String url) throws IllegalLanguageException, IOException {
        jmxManager.markNewUrlReceived();
        long tempTime = System.currentTimeMillis();
        String text = document.text();
        WebDocument webDocument = new WebDocument();
        webdocCreateTime.add((float) (System.currentTimeMillis() - tempTime) / 1000);
        tempTime = System.currentTimeMillis();
        checkLanguage(document, text);
        languagePassed.increment();
        langDetectTime.add((float) (System.currentTimeMillis() - tempTime) / 1000);
        jmxManager.markNewLanguagePassed();
        Link[] links = UrlHandler.getLinks(document.getElementsByTag("a"), new URL(url).getHost());
        webDocument.setTextDoc(text);
        webDocument.setTitle(document.title());
        webDocument.setPageLink(url);
        webDocument.setLinks(new ArrayList<>(Arrays.asList(links)));
        crawledPage.increment();
        jmxManager.markNewCrawledPage();

        return webDocument;
    }

    private void checkLanguage(Document document, String text) throws IllegalLanguageException {
        langDetector.languageCheck(text);
    }
}

