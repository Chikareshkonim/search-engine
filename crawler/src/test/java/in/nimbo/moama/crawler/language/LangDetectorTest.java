package in.nimbo.moama.crawler.language;

import in.nimbo.moama.exception.IllegalLanguageException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

public class LangDetectorTest {

    @BeforeClass
    public static void setUp() throws Exception {
        String recourseAddress=new File(LangDetector.class.getProtectionDomain().getCodeSource().getLocation().toURI())
                .getPath().concat("/../src/main/resources/");

        LangDetector.getInstance().profileLoad(recourseAddress+"profiles");
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