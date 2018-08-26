package in.nimbo.moama.template;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Objects;

public class Template implements Serializable {
    private String attValue;
    private String domain;
    private String funcName;
    private String dateFormatString;
    private String newsTag;
    private transient SimpleDateFormat dateFormatter;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Template)) return false;
        Template template = (Template) o;
        return Objects.equals(getAttributeValue(), template.getAttributeValue()) &&
                Objects.equals(getFuncName(), template.getFuncName()) &&
                Objects.equals(getDateFormatString(), template.getDateFormatString());
    }

    public Template( String attrModel,String attrValue, String dateFormat, String domain,String newsTag) {
        this.attValue = attrValue;
        this.domain = domain;
        switch (attrModel.toLowerCase()) {
            case "id":
                funcName = "getElementById";
                break;
            default:
                funcName = "getElementsBy" + attrModel;
                break;
        }
        this.newsTag=newsTag;
        this.dateFormatString = dateFormat;
        this.dateFormatter = new SimpleDateFormat(dateFormat, Locale.ENGLISH);
    }

    public SimpleDateFormat getDateFormatter() {
        return dateFormatter;
    }

    public String getDateFormatString() {
        return dateFormatString;
    }

    public String getAttributeValue() {
        return attValue;
    }

    public String getFuncName() {
        return funcName;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public void setNewsTag(String newsTag) {
        this.newsTag = newsTag;
    }

    public String getNewsTag() {
        return newsTag;
    }

    @Override
    public String toString() {
        return "Template{" +
                "attValue='" + attValue + '\'' +
                ", domain='" + domain + '\'' +
                ", funcName='" + funcName + '\'' +
                ", dateFormatString='" + dateFormatString + '\'' +
                ", newsTag='" + newsTag + '\'' +
                '}';
    }
}
