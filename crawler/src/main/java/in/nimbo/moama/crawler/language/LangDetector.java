package in.nimbo.moama.crawler.language;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import in.nimbo.moama.exception.IllegalLanguageException;


public class LangDetector {
    private static LangDetector ourInstance=new LangDetector();
    private LangDetector() {
    }

    public static LangDetector getInstance() {
        return ourInstance;
    }
    public void profileLoad(String recourseAddress){
        try {
            DetectorFactory.loadProfile(recourseAddress);
        } catch (LangDetectException ignored) {
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