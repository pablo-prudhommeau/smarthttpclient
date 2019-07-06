package fr.prudhommeau.smarthttpclient.core;

import fr.prudhommeau.smarthttpclient.bean.Proxy;
import fr.prudhommeau.smarthttpclient.exceptions.HttpRequestThreadReadTimeException;
import fr.prudhommeau.smarthttpclient.exceptions.HttpRequestThreadResponseTimeException;
import fr.prudhommeau.threadpoolmanager.SmartThread;
import fr.prudhommeau.threadpoolmanager.SmartThreadPool;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.AAAARecord;
import org.xbill.DNS.ARecord;
import org.xbill.DNS.Lookup;
import org.xbill.DNS.Type;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardProtocolFamily;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

public class HttpClientManager {

    public static final int DEFAULT_REQUEST_TIMEOUT_IN_MILLISECONDS = 30000;

    public static final int NUMBER_OF_RETRIES_WARNING_THRESHOLD = 5;
    public static final int MAXIMUM_NUMBER_OF_REDIRECTS = 5;

    public static final String HTTP_CONTEXT_ATTRIBUTE_STANDARD_KEY_HTTP_ROUTE = "http.route";
    public static final String HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_HTTP_PROXY = "custom.http-proxy";
    public static final String HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_SOCKS_PROXY = "custom.socks-proxy";
    public static final String HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_FORCE_PROTOCOL_FAMILY = "custom.force-protocol-family";

    private static final Logger logger = LoggerFactory.getLogger(HttpClientManager.class);
    private static final ProxyPool PROXY_POOL = new ProxyPool();
    private static final String DEAD_HTTP_REQUEST_THREAD_TIMER_THREAD_NAME = "DeadHttpRequestThreadTimer";

    private final SmartThreadPool smartThreadPool = new SmartThreadPool();
    private final List<HttpRequestThread.OnHttpThreadResponseListener> httpThreadResponseListenerList = Collections.synchronizedList(new ArrayList<>());
    private final List<HttpRequestThread.OnHttpThreadDetailedResponseListener> httpThreadDetailedResponseListenerList = Collections.synchronizedList(new ArrayList<>());
    private final List<HttpRequestThread.OnHttpThreadRawResponseListener> httpThreadRawResponseListenerList = Collections.synchronizedList(new ArrayList<>());
    private final List<HttpRequestThread.OnHttpThreadErrorListener> httpThreadErrorListenerList = Collections.synchronizedList(new ArrayList<>());
    private final Map<Object, HttpRequestThread.OnStepHttpThreadResponseListener> stepHttpThreadResponseListenerMap = Collections.synchronizedMap(new HashMap<>());
    private final Map<Object, HttpRequestThread.OnStepHttpThreadDetailedResponseListener> stepHttpThreadDetailedResponseListenerMap = Collections.synchronizedMap(new HashMap<>());
    private final Map<Object, HttpRequestThread.OnStepHttpThreadRawResponseListener> stepHttpThreadRawResponseListenerMap = Collections.synchronizedMap(new HashMap<>());
    private final Map<Object, HttpRequestThread.OnStepHttpThreadErrorListener> stepHttpThreadErrorListenerMap = Collections.synchronizedMap(new HashMap<>());
    private final List<SmartThreadPool.ThreadPoolEmptyEventListener> threadPoolEmptyEventListenerList = Collections.synchronizedList(new ArrayList<>());

    private PoolingHttpClientConnectionManager connectionManager;
    private Class<? extends Proxifiable> defaultProxifiable;
    private Object initiator;
    private int requestTimeoutInMilliseconds = DEFAULT_REQUEST_TIMEOUT_IN_MILLISECONDS;

    public HttpClientManager(Object initiator) {
        this.initiator = initiator;
        smartThreadPool.setInitiator(this.initiator);

        configureDebug(true);

        PlainConnectionSocketFactory socksConnectionManagerHttpConnectionSocketFactory = new PlainConnectionSocketFactory() {
            @Override
            public Socket createSocket(final HttpContext context) throws IOException {
                if (context.getAttribute(HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_SOCKS_PROXY) != null) {
                    Proxy socksProxy = (Proxy) context.getAttribute(HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_SOCKS_PROXY);
                    InetSocketAddress inetSocketAddress = new InetSocketAddress(socksProxy.getIp(), Integer.parseInt(socksProxy.getPort()));
                    java.net.Proxy proxy = new java.net.Proxy(java.net.Proxy.Type.SOCKS, inetSocketAddress);
                    return new Socket(proxy);
                }
                return super.createSocket(context);
            }

            @Override
            public Socket connectSocket(int connectTimeout, Socket socket, HttpHost host, InetSocketAddress remoteAddress, InetSocketAddress localAddress, HttpContext context) throws IOException {
                StandardProtocolFamily standardProtocolFamily = (StandardProtocolFamily) context.getAttribute(HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_FORCE_PROTOCOL_FAMILY);
                Proxy httpProxy = (Proxy) context.getAttribute(HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_HTTP_PROXY);
                Proxy socksProxy = (Proxy) context.getAttribute(HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_SOCKS_PROXY);
                if (standardProtocolFamily != null) {
                    if (httpProxy != null) {
                        logger.warn("An attempt of rewriting protocol family to [" + standardProtocolFamily + "] was made but an HTTP proxy [" + httpProxy + "] has also been configured. Ignoring standard protocol family rewrite... ");
                        return super.connectSocket(connectTimeout, socket, host, remoteAddress, localAddress, context);
                    } else if (socksProxy != null) {
                        logger.debug("An attempt of rewriting protocol family to [" + standardProtocolFamily + "] was made but a SOCKS proxy [" + socksProxy + "] has also been configured. Ignoring standard protocol family rewrite... ");
                        return super.connectSocket(connectTimeout, socket, host, remoteAddress, localAddress, context);
                    }
                    HttpRoute httpRoute = (HttpRoute) context.getAttribute(HTTP_CONTEXT_ATTRIBUTE_STANDARD_KEY_HTTP_ROUTE);
                    HttpHost httpHost = HttpUtils.uriToHttpHost(httpRoute.getTargetHost().toURI());
                    switch (standardProtocolFamily) {
                        case INET:
                            ARecord aRecord = (ARecord) Arrays.stream(new Lookup(httpHost.getHostName(), Type.A).run()).findFirst().orElseThrow();
                            return super.connectSocket(connectTimeout, socket, host, new InetSocketAddress(aRecord.getAddress().getHostAddress(), httpHost.getPort()), localAddress, context);
                        case INET6:
                            AAAARecord aaaaRecord = (AAAARecord) Arrays.stream(new Lookup(httpHost.getHostName(), Type.AAAA).run()).findFirst().orElseThrow();
                            return super.connectSocket(connectTimeout, socket, host, new InetSocketAddress(aaaaRecord.getAddress().getHostAddress(), httpHost.getPort()), localAddress, context);
                        default:
                            throw new IllegalArgumentException();
                    }
                }
                return super.connectSocket(connectTimeout, socket, host, remoteAddress, localAddress, context);
            }
        };

        SSLConnectionSocketFactory socksConnectionManagerHttpsConnectionSocketFactory = new SSLConnectionSocketFactory(SSLContexts.createSystemDefault(), NoopHostnameVerifier.INSTANCE) {
            @Override
            public Socket createSocket(final HttpContext context) throws IOException {
                if (context.getAttribute(HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_SOCKS_PROXY) != null) {
                    Proxy socksProxy = (Proxy) context.getAttribute(HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_SOCKS_PROXY);
                    InetSocketAddress inetSocketAddress = new InetSocketAddress(socksProxy.getIp(), Integer.parseInt(socksProxy.getPort()));
                    java.net.Proxy proxy = new java.net.Proxy(java.net.Proxy.Type.SOCKS, inetSocketAddress);
                    return new Socket(proxy);
                }
                return super.createSocket(context);
            }

            @Override
            public Socket connectSocket(int connectTimeout, Socket socket, HttpHost host, InetSocketAddress remoteAddress, InetSocketAddress localAddress, HttpContext context) throws IOException {
                StandardProtocolFamily standardProtocolFamily = (StandardProtocolFamily) context.getAttribute(HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_FORCE_PROTOCOL_FAMILY);
                Proxy httpProxy = (Proxy) context.getAttribute(HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_HTTP_PROXY);
                Proxy socksProxy = (Proxy) context.getAttribute(HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_SOCKS_PROXY);
                if (standardProtocolFamily != null) {
                    if (httpProxy != null) {
                        logger.debug("An attempt of rewriting protocol family to [" + standardProtocolFamily + "] was made but an HTTP proxy [" + httpProxy + "] has also been configured. Ignoring standard protocol family rewrite... ");
                        return super.connectSocket(connectTimeout, socket, host, remoteAddress, localAddress, context);
                    } else if (socksProxy != null) {
                        logger.debug("An attempt of rewriting protocol family to [" + standardProtocolFamily + "] was made but a SOCKS proxy [" + socksProxy + "] has also been configured. Ignoring standard protocol family rewrite... ");
                        return super.connectSocket(connectTimeout, socket, host, remoteAddress, localAddress, context);
                    }
                    HttpRoute httpRoute = (HttpRoute) context.getAttribute("http.route");
                    HttpHost httpHost = HttpUtils.uriToHttpHost(httpRoute.getTargetHost().toURI());
                    switch (standardProtocolFamily) {
                        case INET:
                            ARecord aRecord = (ARecord) Arrays.stream(new Lookup(httpHost.getHostName(), Type.A).run()).findFirst().orElseThrow();
                            return super.connectSocket(connectTimeout, socket, host, new InetSocketAddress(aRecord.getAddress().getHostAddress(), httpHost.getPort()), localAddress, context);
                        case INET6:
                            AAAARecord aaaaRecord = (AAAARecord) Arrays.stream(new Lookup(httpHost.getHostName(), Type.AAAA).run()).findFirst().orElseThrow();
                            return super.connectSocket(connectTimeout, socket, host, new InetSocketAddress(aaaaRecord.getAddress().getHostAddress(), httpHost.getPort()), localAddress, context);
                        default:
                            throw new IllegalArgumentException();
                    }
                }
                return super.connectSocket(connectTimeout, socket, host, remoteAddress, localAddress, context);
            }
        };

        Registry<ConnectionSocketFactory> socksConnectionManagerSocksRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", socksConnectionManagerHttpConnectionSocketFactory)
                .register("https", socksConnectionManagerHttpsConnectionSocketFactory)
                .build();
        connectionManager = new PoolingHttpClientConnectionManager(socksConnectionManagerSocksRegistry);
        connectionManager.setMaxTotal(requestTimeoutInMilliseconds);
        connectionManager.setDefaultMaxPerRoute(requestTimeoutInMilliseconds);

        SocketConfig socketConfig = SocketConfig.custom()
                .setSoTimeout(requestTimeoutInMilliseconds)
                .build();
        connectionManager.setDefaultSocketConfig(socketConfig);

        Thread deadHttpRequestThreadTimer = new Thread(() -> {
            logger.debug("Start dead http request thread timer for SmartThreadPool [" + smartThreadPool + "]");
            while (!smartThreadPool.isInterrupted()) {
                LocalDateTime now = LocalDateTime.now();
                ReentrantLock lock = getSmartThreadPool().getLock();
                lock.lock();
                try {
                    List<HttpRequestThread> threadToInterruptForResponseTimeThresholdList = new ArrayList<>();
                    List<HttpRequestThread> threadToInterruptForReadTimeThresholdList = new ArrayList<>();
                    for (SmartThread smartThread : getSmartThreadPool().getRunningInstances()) {
                        if (smartThread instanceof HttpRequestThread) {
                            HttpRequestThread httpRequestThread = (HttpRequestThread) smartThread;
                            LocalDateTime requestedOn = httpRequestThread.getRequestedOn();
                            LocalDateTime respondedOn = httpRequestThread.getRespondedOn();
                            LocalDateTime readOn = httpRequestThread.getReadOn();
                            if (requestedOn != null && respondedOn == null) {
                                if (ChronoUnit.MILLIS.between(requestedOn, now) > requestTimeoutInMilliseconds) {
                                    threadToInterruptForResponseTimeThresholdList.add(httpRequestThread);
                                }
                            } else if (respondedOn != null && readOn == null) {
                                if (ChronoUnit.MILLIS.between(requestedOn, now) > requestTimeoutInMilliseconds) {
                                    threadToInterruptForReadTimeThresholdList.add(httpRequestThread);

                                }
                            }
                        }
                    }
                    for (HttpRequestThread httpRequestThread : threadToInterruptForResponseTimeThresholdList) {
                        HttpRequestThreadResponseTimeException exception = new HttpRequestThreadResponseTimeException("HTTP response exceeded " + requestTimeoutInMilliseconds + " milliseconds");
                        httpRequestThread.executeHttpRequestThreadRetryStrategy(exception);
                        logger.debug("Stopping " + httpRequestThread + " because HTTP response exceeded " + requestTimeoutInMilliseconds + " milliseconds");
                    }
                    for (HttpRequestThread httpRequestThread : threadToInterruptForReadTimeThresholdList) {
                        HttpRequestThreadReadTimeException exception = new HttpRequestThreadReadTimeException("HTTP response read exceeded " + requestTimeoutInMilliseconds + " milliseconds");
                        httpRequestThread.executeHttpRequestThreadRetryStrategy(exception);
                        logger.debug("Stopping " + httpRequestThread + " because HTTP response read exceeded " + requestTimeoutInMilliseconds + " milliseconds");
                    }
                } finally {
                    lock.unlock();
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            logger.debug("Stop dead http request thread timer for SmartThreadPool [" + smartThreadPool + "]");
        });
        deadHttpRequestThreadTimer.setName(DEAD_HTTP_REQUEST_THREAD_TIMER_THREAD_NAME);
        deadHttpRequestThreadTimer.start();
    }

    public static ProxyPool getProxyPool() {
        return PROXY_POOL;
    }

    public CloseableHttpResponse launchSynchronousHttpRequestThread(HttpRequestThread httpRequestThread) throws IOException {
        httpRequestThread.setHttpClientManager(this);
        httpRequestThread.buildRequest();
        return httpRequestThread.getClient().execute(httpRequestThread.getUriRequest(), httpRequestThread.getContext());
    }

    public void launchHttpRequestThread(HttpRequestThread httpRequestThread) {
        if (httpRequestThread.getProxifiable() == null) {
            httpRequestThread.setProxifiable(getDefaultProxifiable());
        }
        httpRequestThread.setHttpClientManager(this);
        if (httpRequestThread.isRandomProxied()) {
            if (!PROXY_POOL.existsProxies(httpRequestThread.getProxifiable())) {
                throw new RuntimeException("Proxy pool does not contain eligible proxy for [" + this.getDefaultProxifiable() + "]");
            }
            Proxy proxy = PROXY_POOL.pickNext(httpRequestThread.getProxifiable(), httpRequestThread.getForcedProtocolFamily());
            httpRequestThread.setProxy(proxy);
            launchInternalHttpRequestThread(httpRequestThread);
        } else {
            launchInternalHttpRequestThread(httpRequestThread);
        }
    }

    private void launchInternalHttpRequestThread(HttpRequestThread httpRequestThread) {
        httpRequestThread.buildRequest();
        httpRequestThread.launchRequest(this);
    }

    public PoolingHttpClientConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void configureDebug(boolean enabled) {
        if (enabled) {
            java.util.logging.Logger.getLogger("org.apache.http.wire").setLevel(Level.FINEST);
            java.util.logging.Logger.getLogger("org.apache.http.headers").setLevel(Level.FINEST);
            System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.SimpleLog");
            System.setProperty("org.apache.commons.logging.simplelog.showdatetime", "true");
            System.setProperty("org.apache.commons.logging.simplelog.log.httpclient.wire", "error");
            System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http", "error");
            System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.headers", "debug");
        } else {
            java.util.logging.Logger.getLogger("org.apache.http").setLevel(Level.OFF);
        }
    }

    public void registerHttpThreadResponseListener(HttpRequestThread.OnHttpThreadResponseListener httpThreadResponseListener) {
        initiator = httpThreadResponseListener;
        httpThreadResponseListenerList.add(httpThreadResponseListener);
    }

    public void registerHttpThreadDetailedResponseListener(HttpRequestThread.OnHttpThreadDetailedResponseListener httpThreadDetailedResponseListener) {
        httpThreadDetailedResponseListenerList.add(httpThreadDetailedResponseListener);
    }

    public void registerHttpThreadRawResponseListener(HttpRequestThread.OnHttpThreadRawResponseListener onHttpThreadRawResponseListener) {
        httpThreadRawResponseListenerList.add(onHttpThreadRawResponseListener);
    }

    public void registerHttpThreadErrorListener(HttpRequestThread.OnHttpThreadErrorListener httpThreadErrorListener) {
        httpThreadErrorListenerList.add(httpThreadErrorListener);
    }

    public void registerStepHttpThreadResponseListener(Object step, HttpRequestThread.OnStepHttpThreadResponseListener onStepHttpThreadResponseListener) {
        stepHttpThreadResponseListenerMap.put(step, onStepHttpThreadResponseListener);
    }

    public void registerStepHttpThreadDetailedResponseListener(Object step, HttpRequestThread.OnStepHttpThreadDetailedResponseListener onStepHttpThreadDetailedResponseListener) {
        stepHttpThreadDetailedResponseListenerMap.put(step, onStepHttpThreadDetailedResponseListener);
    }

    public void registerStepHttpThreadRawResponseListener(Object step, HttpRequestThread.OnStepHttpThreadRawResponseListener onStepHttpThreadRawResponseListener) {
        stepHttpThreadRawResponseListenerMap.put(step, onStepHttpThreadRawResponseListener);
    }

    public void registerStepHttpThreadErrorListener(Object step, HttpRequestThread.OnStepHttpThreadErrorListener onStepHttpThreadErrorListener) {
        stepHttpThreadErrorListenerMap.put(step, onStepHttpThreadErrorListener);
    }

    public List<HttpRequestThread.OnHttpThreadResponseListener> getHttpThreadResponseListenerList() {
        return httpThreadResponseListenerList;
    }

    public List<HttpRequestThread.OnHttpThreadDetailedResponseListener> getHttpThreadDetailedResponseListenerList() {
        return httpThreadDetailedResponseListenerList;
    }

    public List<HttpRequestThread.OnHttpThreadRawResponseListener> getHttpThreadRawResponseListenerList() {
        return httpThreadRawResponseListenerList;
    }

    public List<HttpRequestThread.OnHttpThreadErrorListener> getHttpThreadErrorListenerList() {
        return httpThreadErrorListenerList;
    }

    public Map<Object, HttpRequestThread.OnStepHttpThreadResponseListener> getStepHttpThreadResponseListenerMap() {
        return stepHttpThreadResponseListenerMap;
    }

    public Map<Object, HttpRequestThread.OnStepHttpThreadDetailedResponseListener> getStepHttpThreadDetailedResponseListenerMap() {
        return stepHttpThreadDetailedResponseListenerMap;
    }

    public Map<Object, HttpRequestThread.OnStepHttpThreadRawResponseListener> getStepHttpThreadRawResponseListenerMap() {
        return stepHttpThreadRawResponseListenerMap;
    }

    public Map<Object, HttpRequestThread.OnStepHttpThreadErrorListener> getStepHttpThreadErrorListenerMap() {
        return stepHttpThreadErrorListenerMap;
    }

    public List<SmartThreadPool.ThreadPoolEmptyEventListener> getThreadPoolEmptyEventListenerList() {
        return threadPoolEmptyEventListenerList;
    }

    public SmartThreadPool getSmartThreadPool() {
        return smartThreadPool;
    }

    public Object getInitiator() {
        return initiator;
    }

    public void setInitiator(Object initiator) {
        this.initiator = initiator;
    }

    public Class<? extends Proxifiable> getDefaultProxifiable() {
        return defaultProxifiable;
    }

    public void setDefaultProxifiable(Class<? extends Proxifiable> defaultProxifiable) {
        this.defaultProxifiable = defaultProxifiable;
    }

    public int getRequestTimeoutInMilliseconds() {
        return requestTimeoutInMilliseconds;
    }

    public void setRequestTimeoutInMilliseconds(int requestTimeoutInMilliseconds) {
        this.requestTimeoutInMilliseconds = requestTimeoutInMilliseconds;
    }
}
