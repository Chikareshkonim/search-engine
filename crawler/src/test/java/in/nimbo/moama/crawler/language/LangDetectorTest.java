package in.nimbo.moama.crawler.language;

import com.optimaize.langdetect.LanguageDetector;
import in.nimbo.moama.exception.IllegalLanguageException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class LangDetectorTest {

    @BeforeClass
    public static void setUp() throws Exception {
        LangDetector.getInstance().profileLoad();
    }

    @Test(expected = IllegalLanguageException.class)
    public void languageCheck() throws IllegalLanguageException {
        LangDetector.getInstance().languageCheck("سلام خوبی من که خیلی خوبم به لطف خدا" );
    }

    @Test
    public void languageCheck2() throws IllegalLanguageException {
        LangDetector.getInstance().languageCheck("oh my god you are best in by world and my eye" );
    }
}