package fr.prudhommeau.smarthttpclient.bean;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class RouteProperty {

    private int maxCall;
    private boolean secured;
    private String ip;

    public boolean isSecured() {
        return secured;
    }

    public void setSecured(boolean secured) {
        this.secured = secured;
    }

    public int getMaxCall() {
        return maxCall;
    }

    public void setMaxCall(int maxCall) {
        this.maxCall = maxCall;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

}
