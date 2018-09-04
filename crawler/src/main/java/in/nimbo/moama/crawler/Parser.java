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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;

import static java.lang.Integer.min;
import static sun.swing.MenuItemLayoutHelper.max;

public class Parser {
    private static LangDetector langDetector;
    private static Parser ourInstance=new Parser();
    private static DuplicateHandler duplicateChecker;
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
//        jmxManager.markNewUrlReceived();
        long tempTime = System.currentTimeMillis();
        String text = document.text();
        WebDocument webDocument = new WebDocument();
        webdocCreateTime.add((float) (System.currentTimeMillis() - tempTime) / 1000);
        tempTime = System.currentTimeMillis();
        checkLanguage(document, text);
        languagePassed.increment();
        langDetectTime.add((float) (System.currentTimeMillis() - tempTime) / 1000);
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
//        if (text.length()<100){
//            langDetector.languageCheck(text);
//        }
//        try {
//            String lang = document.getElementsByAttribute("lang").get(0).attr("lang").toLowerCase();
//            if (!(lang.equals("en")||lang.startsWith("en-")||lang.endsWith("-en"))) {
//                throw new IllegalLanguageException();
//            }
//            langDetector.languageCheck(text.substring(text.length()/2,min(text.length()-1,text.length()/2+100)));
//        } catch (RuntimeException e) {
//            langDetector.languageCheck(text.substring(text.length()/2,min(text.length()-1,text.length()/2+100)));
        //    //getElementsByAttribute throws a NullPointerException if document doesn't have lang tag
//        }
        langDetector.languageCheck(text);
    }
}

