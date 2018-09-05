package in.nimbo.moama.crawler.language;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.google.common.base.Optional;
import com.optimaize.langdetect.DetectedLanguage;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;
import in.nimbo.moama.exception.IllegalLanguageException;

import java.io.IOException;
import java.util.List;


public class LangDetector {
    private static LangDetector ourInstance=new LangDetector();
    private LangDetector() {
    }

    public static LangDetector getInstance() {
        return ourInstance;
    }
    public void profileLoad() {

        try {
            System.out.println(System.getProperty("user.dir"));
            DetectorFactory.loadProfile(System.getProperty("user.dir")+"/crawler/src/main/resources/profiles");
        } catch (LangDetectException e) {
            e.printStackTrace();
        }
    }


    public  void languageCheck(String text) throws IllegalLanguageException {
        Detector detector;
        try {
            detector = DetectorFactory.create();
            detector.append(text);
            if (!detector.detect().equals("en")) {
                throw new IllegalLanguageException();
            }
        } catch (LangDetectException e) {
            throw new IllegalLanguageException();
        }
    }

}