package fr.prudhommeau.smarthttpclient.bean;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.http.HttpHost;

import java.io.Serializable;
import java.util.Objects;

public class Proxy implements Serializable {

    private String ip;
    private String port;
    private ProxyType type;

    public HttpHost asHost() {
        return new HttpHost(ip, Integer.parseInt(port));
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public ProxyType getType() {
        return type;
    }

    public void setType(ProxyType type) {
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Proxy proxy = (Proxy) o;
        return Objects.equals(ip, proxy.ip) &&
                Objects.equals(port, proxy.port) &&
                type == proxy.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, port, type);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }

}
