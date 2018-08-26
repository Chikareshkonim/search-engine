package in.nimbo.moama;

import java.util.LinkedHashMap;

public class RSSs {

    private static RSSs ourInstance = new RSSs();
    public static RSSs getInstance() {
        return ourInstance;
    }
    private RSSs() {
    }
    public LinkedHashMap <String,String > rssToDomainMap;

    public void loadRSSs(){
        // TODO: 8/17/18
    }
    public void saveRSSs(){
        // TODO: 8/17/18
    }
}
