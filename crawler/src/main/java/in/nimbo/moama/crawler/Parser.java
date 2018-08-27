package in.nimbo.moama.crawler;

import in.nimbo.moama.document.Link;
import in.nimbo.moama.UrlHandler;
import in.nimbo.moama.document.WebDocument;
import in.nimbo.moama.crawler.domainvalidation.DomainFrequencyHandler;
import in.nimbo.moama.crawler.domainvalidation.DuplicateLinkHandler;
import in.nimbo.moama.crawler.language.LangDetector;
import in.nimbo.moama.exception.DomainFrequencyException;
import in.nimbo.moama.exception.DuplicateLinkException;
import in.nimbo.moama.exception.IllegalLanguageException;
import in.nimbo.moama.exception.URLException;
import in.nimbo.moama.metrics.Metrics;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;

public class Parser {
    private static Logger errorLogger = Logger.getLogger("error");
    private static LangDetector langDetector;
    private static Parser parser;
    private static DuplicateLinkHandler duplicateLinkHandler = DuplicateLinkHandler.getInstance();
    private static DomainFrequencyHandler domainTimeHandler = DomainFrequencyHandler.getInstance();

    public synchronized static Parser getInstance() {
        if (parser == null)
            parser = new Parser();
        return parser;
    }

    public static void setLangDetector(LangDetector langDetector) {
        Parser.langDetector = langDetector;
    }

    public WebDocument parse(String url) throws IllegalLanguageException, IOException, URLException,
            DuplicateLinkException, DomainFrequencyException {
        if (url == null) {
            errorLogger.error("number of null" + Metrics.numberOfNull++);
            throw new URLException();
        } else if (!domainTimeHandler.isAllow(url)) {
            errorLogger.error("take less than 30s to request to " + url);
            Metrics.numberOfDomainError++;
            throw new DomainFrequencyException();
        }
        String text = null;
        if (duplicateLinkHandler.isDuplicate(url)) {
            errorLogger.error(url + " is duplicate");
            Metrics.numberOfDuplicate++;
            throw new DuplicateLinkException();
        }
        try {
            Document document = Jsoup.connect(url).validateTLSCertificates(false).get();
            duplicateLinkHandler.confirm(url);
            Metrics.numberOfUrlReceived++;
            WebDocument webDocument = new WebDocument();
            text = document.text();
            checkLanguage(document, text);
            Metrics.numberOfLanguagePassed++;
            Link[] links = UrlHandler.getLinks(document.getElementsByTag("a"), new URL(url).getHost());
            webDocument.setTextDoc(text);
            webDocument.setTitle(document.title());
            webDocument.setPageLink(url);
            webDocument.setLinks(new ArrayList<>(Arrays.asList(links)));
            Metrics.numberOFCrawledPage++;
            return webDocument;
        } catch (MalformedURLException e) {
            errorLogger.error(url + " is malformatted!");
            throw e;
        } catch (IOException e) {
            errorLogger.error("Jsoup connection to " + url + " failed");
            throw e;
        } catch (IllegalLanguageException e) {
            errorLogger.error("Couldn't recognize url language!" + url);
            throw e;
        }
    }

    private void checkLanguage(Document document, String text) throws IllegalLanguageException {
        try {
            String lang = document.getElementsByAttribute("lang").get(0).attr("lang").toLowerCase();
            if (lang.equals("en") || lang.startsWith("en-")) {
                return;
            }
            throw new IllegalLanguageException();
        } catch (RuntimeException e) {
            //getElementsByAttribute throws a NullPointerException if document doesn't have lang tag
            langDetector.languageCheck(text);
        }
    }
}

