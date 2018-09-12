package in.nimbo.moama.crawler;

import in.nimbo.moama.Initializer;
import in.nimbo.moama.document.WebDocument;
import in.nimbo.moama.exception.IllegalLanguageException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ParserTest {
    @Before
    public void setUp() throws Exception {
        Initializer.initialize();
    }

    @Test
    public void parse() throws IOException, IllegalLanguageException {
        Document document = Jsoup.parse("<body>hi how old are you man . you know im the best in my zone and im comfort very well</body>");
        WebDocument webDocument = Parser.getInstance().parse(document, "http://salam.ir/");
        Assert.assertEquals(webDocument.getTextDoc(), "hi how old are you man . you know im the best in my zone and im comfort very well");
        Assert.assertEquals(webDocument.getLinks().size(), 0);
    }

    @Test(expected = IllegalLanguageException.class)
    public void parse1() throws IOException, IllegalLanguageException {
        Document document = Jsoup.parse("<body>سلام خوبی چطوری؟   خیلی خوبی تو</body>");
        Parser.getInstance().parse(document, "http://salam.ir/");
    }

    @Test
    public void parse2() throws IOException, IllegalLanguageException {
        Document document = Jsoup.parse("<html><body><a href=\"/fa/contacts\" title=\"contacts\">hi to world am the" +
                " best man who know you well</a></body></html>");
        WebDocument webDocument = Parser.getInstance().parse(document, "http://salam.ir/");
        System.out.println(webDocument.getTextDoc());

    }

}