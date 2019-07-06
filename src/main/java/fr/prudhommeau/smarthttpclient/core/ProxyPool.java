package fr.prudhommeau.smarthttpclient.core;

import fr.prudhommeau.smarthttpclient.bean.Proxy;

import java.net.*;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class ProxyPool {

    private final Map<Class<? extends Proxifiable>, List<ProxyInfo>> proxifiableToProxyInfoListMap = new HashMap<>();

    public static class ProxyInfo {

        private Proxy proxy;
        private Long numberOfConnectionFailures;
        private Long numberOfUsages;
        private Double averageResponseTimeInMilliseconds;

        public Proxy getProxy() {
            return proxy;
        }

        public Long getNumberOfConnectionFailures() {
            return numberOfConnectionFailures;
        }

        public Long getNumberOfUsages() {
            return numberOfUsages;
        }

        public Double getAverageResponseTimeInMilliseconds() {
            return averageResponseTimeInMilliseconds;
        }

        public void setProxy(Proxy proxy) {
            this.proxy = proxy;
        }

        public void setNumberOfConnectionFailures(Long numberOfConnectionFailures) {
            this.numberOfConnectionFailures = numberOfConnectionFailures;
        }

        public void setNumberOfUsages(Long numberOfUsages) {
            this.numberOfUsages = numberOfUsages;
        }

        public void setAverageResponseTimeInMilliseconds(Double averageResponseTimeInMilliseconds) {
            this.averageResponseTimeInMilliseconds = averageResponseTimeInMilliseconds;
        }
    }

    public Proxy pickNext(Class<? extends Proxifiable> proxifiable, ProtocolFamily forcedProtocolFamily) {
        synchronized (proxifiableToProxyInfoListMap) {
            List<ProxyInfo> sortedProxyInfoList = proxifiableToProxyInfoListMap.get(proxifiable).stream()
                    .sorted(Comparator.comparing(ProxyInfo::getNumberOfConnectionFailures)
                            .thenComparing(ProxyInfo::getAverageResponseTimeInMilliseconds)
                            .thenComparing(ProxyInfo::getNumberOfUsages))
                    .collect(Collectors.toList());
            if (forcedProtocolFamily != null) {
                if (forcedProtocolFamily == StandardProtocolFamily.INET) {
                    boolean noneInetProxy = sortedProxyInfoList.stream().noneMatch(proxyInfo -> {
                        try {
                            return InetAddress.getByName(proxyInfo.getProxy().getIp()) instanceof Inet4Address;
                        } catch (UnknownHostException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    if (noneInetProxy) {
                        throw new IllegalStateException("Proxy pool does not contain any eligible proxy");
                    }
                } else if (forcedProtocolFamily == StandardProtocolFamily.INET6) {
                    boolean noneInet6Proxy = sortedProxyInfoList.stream().noneMatch(proxyInfo -> {
                        try {
                            return InetAddress.getByName(proxyInfo.getProxy().getIp()) instanceof Inet6Address;
                        } catch (UnknownHostException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    if (noneInet6Proxy) {
                        throw new IllegalStateException("Proxy pool does not contain any eligible proxy");
                    }
                }
            }
            if (sortedProxyInfoList.size() >= 1) {
                ProxyInfo eligibleProxyInfo = sortedProxyInfoList.get(0);
                eligibleProxyInfo.setNumberOfUsages(eligibleProxyInfo.getNumberOfUsages() + 1);
                return eligibleProxyInfo.getProxy();
            } else {
                throw new RuntimeException("No proxy available");
            }
        }
    }

    public Proxy pickNext(Class<? extends Proxifiable> proxifiable) {
        return pickNext(proxifiable, null);
    }

    public void incrementNumberOfConnectionFailures(Proxy proxy, Class<? extends Proxifiable> proxifiable) {
        synchronized (proxifiableToProxyInfoListMap) {
            for (ProxyInfo proxyInfo : proxifiableToProxyInfoListMap.get(proxifiable)) {
                if (proxyInfo.getProxy().equals(proxy)) {
                    proxyInfo.setNumberOfConnectionFailures(proxyInfo.getNumberOfConnectionFailures() + 1);
                }
            }
        }
    }

    public void loadProxyList(Map<Class<? extends Proxifiable>, List<ProxyInfo>> proxifiableToProxyInfoListMap) {
        for (Map.Entry<Class<? extends Proxifiable>, List<ProxyInfo>> entry : proxifiableToProxyInfoListMap.entrySet()) {
            this.proxifiableToProxyInfoListMap.put(entry.getKey(), new CopyOnWriteArrayList<>(entry.getValue()));
        }
    }

    public boolean existsProxies(Class<? extends Proxifiable> proxifiable) {
        return this.proxifiableToProxyInfoListMap.containsKey(proxifiable) && this.proxifiableToProxyInfoListMap.get(proxifiable).size() > 0;
    }

}
