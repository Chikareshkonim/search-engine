package in.nimbo.moama.fetcher;

import in.nimbo.moama.configmanager.ConfigManager;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;

import static org.junit.Assert.*;

public class RSSParserTest {

    @Test
    public void uniqueDateFormat() throws ParseException, IOException {
        ConfigManager.getInstance().load(getClass().getResourceAsStream("/news.properties"), ConfigManager.FileType.PROPERTIES);
        String oddDateFormat = "dd MMM yyyy HH:mm:ss Z";
        String oddDate = "03 Sep 2018 14:19:59 +0430";
        assertEquals("Mon, 03 Sep 2018 14:19:59 +0430", RSSParser.uniqueDateFormat(oddDateFormat, oddDate));
    }
}