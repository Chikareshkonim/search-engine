package in.nimbo.moama.metrics;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;

public class JMXManager {
    private static JMXManager ourInstance = new JMXManager();
    private MetricRegistry metrics = new MetricRegistry();
    private Meter numberOfUrlReceived = metrics.meter("numberOfUrlReceived");
    private Meter numberOfNull = metrics.meter("numberOfNull");
    private Meter numberOfDuplicate = metrics.meter("numberOfDuplicate");
    private Meter numberOfDomainError = metrics.meter("numberOfDomainError");
    private Meter numberOfCrawledPage = metrics.meter("numberOfCrawledPage");
    private Meter numberOfLanguagePassed = metrics.meter("numberOfLanguagePassed");
    private Meter numberOfPagesAddedToElastic = metrics.meter("numberOfPagesAddedToElastic");
    private Meter numberOfPagesAddedToHBase = metrics.meter("numberOfPagesAddedToHBase");
    private Meter numberOfComplete = metrics.meter("numberOfComplete");


    public static JMXManager getInstance(){
        return ourInstance;
    }

    private JMXManager(){
        JmxReporter reporter = JmxReporter.forRegistry(metrics).build();
        reporter.start();
    }

    public void markNewUrlReceived(){
        numberOfUrlReceived.mark();
    }

    public void markNewNull(){
        numberOfNull.mark();
    }

    public void markNewDuplicate(){
        numberOfDuplicate.mark();
    }

    public void markNewDomainError(){
        numberOfDomainError.mark();
    }

    public void markNewCrawledPage(){
        numberOfCrawledPage.mark();
    }

    public void markNewLanguagePassed(){
        numberOfLanguagePassed.mark();
    }

    public void markNewAddedToElastic(){
        numberOfPagesAddedToElastic.mark();
    }

    public void markNewAddedToHBase(){
        numberOfPagesAddedToHBase.mark();
    }
    public void markNewComplete(){
        numberOfComplete.mark();
    }

}
