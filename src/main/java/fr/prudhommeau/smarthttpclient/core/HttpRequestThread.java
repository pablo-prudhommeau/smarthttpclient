package fr.prudhommeau.smarthttpclient.core;

import fr.prudhommeau.commons.StringUtils;
import fr.prudhommeau.smarthttpclient.bean.HttpMethod;
import fr.prudhommeau.smarthttpclient.bean.Proxy;
import fr.prudhommeau.smarthttpclient.bean.ProxyType;
import fr.prudhommeau.threadpoolmanager.SmartThread;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.http.*;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.*;
import org.apache.http.impl.conn.DefaultProxyRoutePlanner;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.EOFException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.StandardProtocolFamily;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.*;
import java.util.zip.ZipException;

public class HttpRequestThread<T> extends SmartThread implements SmartThread.OnThreadRunningListener, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(HttpRequestThread.class);

    private final List<Header> responseHeaders = new ArrayList<>();
    private CloseableHttpClient client;
    private HttpClientManager httpClientManager;
    private String uri;
    private HttpUriRequest uriRequest;
    private T requestId;
    private HashMap<String, String> postBodyParams = new HashMap<>();
    private String body;
    private boolean retryWhenConnectionFail = true;
    private Proxy proxy;
    private boolean randomProxied;
    private HttpMethod method;
    private HashMap<String, String> customHeaders = new HashMap<>();
    private HttpClientContext context;
    private LocalDateTime requestedOn;
    private LocalDateTime respondedOn;
    private LocalDateTime readOn;
    private int numberOfRetries;
    private CookieStore cookieStore = new BasicCookieStore();
    private List<RetryHistory> retryHistoryList = new ArrayList<>();
    private Class<? extends Proxifiable> proxifiable;
    private boolean ignoreErrors;
    private StandardProtocolFamily forcedProtocolFamily;
    private String forcedRemoteAddress;

    public HttpRequestThread() {
        setName("HttpRequestThread-" + uuid);
    }

    public interface OnHttpThreadResponseListener<T> {
        void onHttpThreadResponse(HttpRequestThread requestThreadInstance, String response, Map<String, Object> metadata, T requestId);
    }

    public interface OnHttpThreadDetailedResponseListener<T> {
        void onHttpThreadResponse(HttpRequestThread requestThreadInstance, String response, Map<String, Object> metadata, T requestId);
    }

    public interface OnHttpThreadRawResponseListener<T> {
        void onHttpThreadResponse(HttpRequestThread requestThreadInstance, byte[] response, Map<String, Object> metadata, T requestId);
    }

    public interface OnHttpThreadErrorListener<T> {
        void onHttpThreadError(Exception exception, HttpRequestThread requestThreadInstance, Map<String, Object> metadata, T requestId);
    }

    public interface OnStepHttpThreadResponseListener<T> {
        void apply(HttpRequestThread requestThreadInstance, String response, Map<String, Object> metadata);
    }

    public interface OnStepHttpThreadDetailedResponseListener<T> {
        void apply(HttpRequestThread requestThreadInstance, String response, Map<String, Object> metadata);
    }

    public interface OnStepHttpThreadRawResponseListener<T> {
        void apply(HttpRequestThread requestThreadInstance, byte[] response, Map<String, Object> metadata);
    }

    public interface OnStepHttpThreadErrorListener<T> {
        void apply(Exception exception, HttpRequestThread requestThreadInstance, Map<String, Object> metadata);
    }

    public static class RetryHistory {

        private Proxy proxy;
        private String errorDetails;

        public Proxy getProxy() {
            return proxy;
        }

        public String getErrorDetails() {
            return errorDetails;
        }

        public void setProxy(Proxy proxy) {
            this.proxy = proxy;
        }

        public void setErrorDetails(String errorDetails) {
            this.errorDetails = errorDetails;
        }

    }

    public static Logger getLogger() {
        return logger;
    }

    public void buildRequest() {
        RequestConfig requestConfig = RequestConfig.custom()
                .setCookieSpec(CookieSpecs.STANDARD)
                .setConnectTimeout(httpClientManager.getRequestTimeoutInMilliseconds())
                .setConnectionRequestTimeout(httpClientManager.getRequestTimeoutInMilliseconds())
                .setSocketTimeout(httpClientManager.getRequestTimeoutInMilliseconds())
                .setMaxRedirects(HttpClientManager.MAXIMUM_NUMBER_OF_REDIRECTS)
                .build();

        SocketConfig socketConfig = SocketConfig.custom()
                .setSoTimeout(httpClientManager.getRequestTimeoutInMilliseconds())
                .build();

        HttpClientBuilder clientBuilder = HttpClients.custom()
                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                .setDefaultRequestConfig(requestConfig)
                .setDefaultSocketConfig(socketConfig)
                .setDefaultCookieStore(cookieStore);

        clientBuilder.setConnectionManager(httpClientManager.getConnectionManager());

        DefaultHttpRequestRetryHandler retryHandler = new DefaultHttpRequestRetryHandler(0, false);
        clientBuilder.setRetryHandler(retryHandler);

        context = HttpClientContext.create();
        context.setCookieStore(cookieStore);

        if (proxy != null && proxy.getType() != null) {
            if (proxy.getType() == ProxyType.HTTP) {
                clientBuilder.setRoutePlanner(new DefaultProxyRoutePlanner(proxy.asHost()));
                context.setAttribute(HttpClientManager.HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_HTTP_PROXY, proxy);
            }
            if (proxy.getType() == ProxyType.SOCKS) {
                context.setAttribute(HttpClientManager.HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_SOCKS_PROXY, proxy);
            }
        }

        if (forcedProtocolFamily != null) {
            context.setAttribute(HttpClientManager.HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_FORCE_PROTOCOL_FAMILY, forcedProtocolFamily);
        }

        HttpHost host = HttpUtils.uriToHttpHost(uri);
        String finalUri;
        if (!StringUtils.isNullOrEmpty(forcedRemoteAddress)) {
            customHeaders.put("Host", host.getHostName());
            finalUri = uri.replace(host.getHostName(), forcedRemoteAddress);
        } else {
            finalUri = uri;
        }

        if (method == null) {
            method = HttpMethod.GET;
        }
        switch (method) {
            case GET:
                uriRequest = new HttpGet(finalUri);
                break;
            case POST:
                uriRequest = new HttpPost(finalUri);
                List<NameValuePair> params = new ArrayList<>();
                for (Map.Entry<String, String> entry : postBodyParams.entrySet()) {
                    params.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
                }
                try {
                    if (body != null) {
                        ((HttpPost) uriRequest).setEntity(new StringEntity(body));
                    } else {
                        ((HttpPost) uriRequest).setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));
                    }
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
                break;
        }

        for (Map.Entry<String, String> header : customHeaders.entrySet()) {
            uriRequest.setHeader(header.getKey(), header.getValue());
        }

        client = clientBuilder.build();
    }

    public void launchRequest(HttpClientManager httpClientManager) {
        setThreadPool(httpClientManager.getSmartThreadPool());
        setListener(this);
        super.launchRequest();
    }

    @Override
    public void onThreadRunning(SmartThread smartThreadInstance, Map<String, Object> metadata) {
        requestedOn = LocalDateTime.now();
        try (CloseableHttpResponse response = client.execute(uriRequest, context)) {
            if (isInterrupted()) {
                return;
            }
            respondedOn = LocalDateTime.now();
            byte[] responseAsByteArray = response.getEntity().getContent().readAllBytes();
            if (isInterrupted()) {
                return;
            }
            readOn = LocalDateTime.now();
            for (OnHttpThreadResponseListener onHttpThreadResponseListener : httpClientManager.getHttpThreadResponseListenerList()) {
                String trimmedResponseAsString = HttpUtils.trim(HttpUtils.readHttpResponseContent(responseAsByteArray));
                onHttpThreadResponseListener.onHttpThreadResponse(this, trimmedResponseAsString, metadata, requestId);
            }
            if (httpClientManager.getStepHttpThreadResponseListenerMap().containsKey(requestId)) {
                String trimmedResponseAsString = HttpUtils.trim(HttpUtils.readHttpResponseContent(responseAsByteArray));
                httpClientManager.getStepHttpThreadResponseListenerMap().get(requestId).apply(this, trimmedResponseAsString, metadata);
            }
            for (OnHttpThreadDetailedResponseListener onHttpThreadDetailedResponseListener : httpClientManager.getHttpThreadDetailedResponseListenerList()) {
                String responseAsString = HttpUtils.readHttpResponseContent(responseAsByteArray);
                responseHeaders.addAll(Arrays.asList(response.getAllHeaders()));
                onHttpThreadDetailedResponseListener.onHttpThreadResponse(this, responseAsString, metadata, requestId);
            }
            if (httpClientManager.getStepHttpThreadDetailedResponseListenerMap().containsKey(requestId)) {
                String responseAsString = HttpUtils.readHttpResponseContent(responseAsByteArray);
                responseHeaders.addAll(Arrays.asList(response.getAllHeaders()));
                httpClientManager.getStepHttpThreadDetailedResponseListenerMap().get(requestId).apply(this, responseAsString, metadata);
            }
            for (OnHttpThreadRawResponseListener onHttpThreadRawResponseListener : httpClientManager.getHttpThreadRawResponseListenerList()) {
                onHttpThreadRawResponseListener.onHttpThreadResponse(this, responseAsByteArray, metadata, requestId);
            }
            if (httpClientManager.getStepHttpThreadRawResponseListenerMap().containsKey(requestId)) {
                httpClientManager.getStepHttpThreadRawResponseListenerMap().get(requestId).apply(this, responseAsByteArray, metadata);
            }
        } catch (TruncatedChunkException | SocketException | SSLException | ConnectTimeoutException | NoHttpResponseException | ConnectionClosedException | ClientProtocolException | SocketTimeoutException | ZipException | EOFException e) {
            executeHttpRequestThreadRetryStrategy(e);
            logger.debug("An exception occurred : " + e.getClass() + " - " + e.getMessage() + " - " + this);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void executeHttpRequestThreadRetryStrategy(Exception exception) {
        if (randomProxied && proxy != null) {
            HttpClientManager.getProxyPool().incrementNumberOfConnectionFailures(proxy, proxifiable);
        }
        if (retryWhenConnectionFail) {
            RetryHistory retryHistory = new RetryHistory();
            retryHistory.setProxy(proxy);
            retryHistory.setErrorDetails(exception.getClass().getName() + " : " + exception.getMessage());
            retryHistoryList.add(retryHistory);

            if (numberOfRetries >= HttpClientManager.NUMBER_OF_RETRIES_WARNING_THRESHOLD) {
                logger.warn("Thread [" + this + "] has been retried more than [" + HttpClientManager.NUMBER_OF_RETRIES_WARNING_THRESHOLD + "]... Retry a last time without proxy...");
                proxy = null;
                randomProxied = false;
                retryWhenConnectionFail = false;
            }

            retryWithAnotherThread();
        }

        boolean hasErrorBeenCatched = false;
        for (OnHttpThreadErrorListener onHttpThreadErrorListener : httpClientManager.getHttpThreadErrorListenerList()) {
            onHttpThreadErrorListener.onHttpThreadError(exception, this, metadata, requestId);
            hasErrorBeenCatched = true;
        }
        if (httpClientManager.getStepHttpThreadErrorListenerMap().containsKey(requestId)) {
            httpClientManager.getStepHttpThreadErrorListenerMap().get(requestId).apply(exception, this, metadata);
            hasErrorBeenCatched = true;
        }
        if (!hasErrorBeenCatched && !ignoreErrors && !retryWhenConnectionFail) {
            throw new RuntimeException(exception);
        }

        if (!retryWhenConnectionFail) {
            httpClientManager.getSmartThreadPool().interruptSmartThread(this);
        }
    }

    @Override
    public void retryWithAnotherThread() {
        if (numberOfRetries == HttpClientManager.NUMBER_OF_RETRIES_WARNING_THRESHOLD) {
            logger.warn("Thread [" + this + "] has been retried more than [" + HttpClientManager.NUMBER_OF_RETRIES_WARNING_THRESHOLD + "]... Retry a last time without proxy...");
            proxy = null;
            randomProxied = false;
        } else if (numberOfRetries > HttpClientManager.NUMBER_OF_RETRIES_WARNING_THRESHOLD) {
            throw new IllegalStateException();
        }

        HttpRequestThread httpRequestThread = new HttpRequestThread();
        httpRequestThread.setUri(uri);
        httpRequestThread.setRequestId(requestId);
        httpRequestThread.setPostBodyParams(postBodyParams);
        httpRequestThread.setBody(body);
        httpRequestThread.setRetryWhenConnectionFail(retryWhenConnectionFail);
        httpRequestThread.setCustomHeaders(customHeaders);
        httpRequestThread.setContext(context);
        httpRequestThread.setRandomProxied(randomProxied);
        httpRequestThread.setNumberOfRetries(numberOfRetries + 1);
        httpRequestThread.setCookieStore(cookieStore);
        httpRequestThread.setMethod(method);
        httpRequestThread.setMetadata(metadata);
        httpRequestThread.setRetryHistoryList(retryHistoryList);
        httpRequestThread.setProxifiable(proxifiable);
        httpRequestThread.setIgnoreErrors(ignoreErrors);
        httpRequestThread.setForcedProtocolFamily(forcedProtocolFamily);
        httpRequestThread.setForcedRemoteAddress(forcedRemoteAddress);
        httpClientManager.launchHttpRequestThread(httpRequestThread);

        httpClientManager.getSmartThreadPool().interruptSmartThread(this);
    }

    public void addMetadata(String key, Object value) {
        metadata.put(key, value);
    }

    public void addPostBodyParam(String key, String value) {
        postBodyParams.put(key, value);
    }

    public void addCustomHeader(String key, String value) {
        customHeaders.put(key, value);
    }

    public CloseableHttpClient getClient() {
        return client;
    }

    public boolean isRandomProxied() {
        return randomProxied;
    }

    public Proxy getProxy() {
        return proxy;
    }

    public String getUri() {
        return uri;
    }

    public HttpClientManager getHttpClientManager() {
        return httpClientManager;
    }

    public HttpClientContext getContext() {
        return context;
    }

    public LocalDateTime getRequestedOn() {
        return requestedOn;
    }

    public LocalDateTime getRespondedOn() {
        return respondedOn;
    }

    public LocalDateTime getReadOn() {
        return readOn;
    }

    public HttpUriRequest getUriRequest() {
        return uriRequest;
    }

    public T getRequestId() {
        return requestId;
    }

    public HashMap<String, String> getPostBodyParams() {
        return postBodyParams;
    }

    public String getBody() {
        return body;
    }

    public boolean isRetryWhenConnectionFail() {
        return retryWhenConnectionFail;
    }

    public HttpMethod getMethod() {
        return method;
    }

    public HashMap<String, String> getCustomHeaders() {
        return customHeaders;
    }

    public int getNumberOfRetries() {
        return numberOfRetries;
    }

    public CookieStore getCookieStore() {
        return cookieStore;
    }

    public List<Header> getResponseHeaders() {
        return responseHeaders;
    }

    public List<RetryHistory> getRetryHistoryList() {
        return retryHistoryList;
    }

    public Class<? extends Proxifiable> getProxifiable() {
        return proxifiable;
    }

    public boolean isIgnoreErrors() {
        return ignoreErrors;
    }

    public void setRandomProxied(boolean randomProxied) {
        this.randomProxied = randomProxied;
    }

    public void setMethod(HttpMethod method) {
        this.method = method;
    }

    public void setRetryWhenConnectionFail(boolean bool) {
        retryWhenConnectionFail = bool;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public void setRequestId(T requestId) {
        this.requestId = requestId;
    }

    public void setHttpClientManager(HttpClientManager httpClientManager) {
        this.httpClientManager = httpClientManager;
    }

    public void setCookieStore(CookieStore cookieStore) {
        this.cookieStore = cookieStore;
    }

    public void setNumberOfRetries(int numberOfRetries) {
        this.numberOfRetries = numberOfRetries;
    }

    public void setClient(CloseableHttpClient client) {
        this.client = client;
    }

    public void setUriRequest(HttpUriRequest uriRequest) {
        this.uriRequest = uriRequest;
    }

    public void setPostBodyParams(HashMap<String, String> postBodyParams) {
        this.postBodyParams = postBodyParams;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public void setProxy(Proxy proxy) {
        this.proxy = proxy;
    }

    public void setCustomHeaders(HashMap<String, String> customHeaders) {
        this.customHeaders = customHeaders;
    }

    public void setContext(HttpClientContext context) {
        this.context = context;
    }

    public void setRequestedOn(LocalDateTime requestedOn) {
        this.requestedOn = requestedOn;
    }

    public void setRespondedOn(LocalDateTime respondedOn) {
        this.respondedOn = respondedOn;
    }

    public void setReadOn(LocalDateTime readOn) {
        this.readOn = readOn;
    }

    public void setRetryHistoryList(List<RetryHistory> retryHistoryList) {
        this.retryHistoryList = retryHistoryList;
    }

    public void setProxifiable(Class<? extends Proxifiable> proxifiable) {
        this.proxifiable = proxifiable;
    }

    public void setIgnoreErrors(boolean ignoreErrors) {
        this.ignoreErrors = ignoreErrors;
    }

    public StandardProtocolFamily getForcedProtocolFamily() {
        return forcedProtocolFamily;
    }

    public void setForcedProtocolFamily(StandardProtocolFamily forcedProtocolFamily) {
        this.forcedProtocolFamily = forcedProtocolFamily;
    }

    public String getForcedRemoteAddress() {
        return forcedRemoteAddress;
    }

    public void setForcedRemoteAddress(String forcedRemoteAddress) {
        this.forcedRemoteAddress = forcedRemoteAddress;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("uri", uri)
                .append("requestId", requestId)
                .append("proxy", proxy)
                .append("method", method)
                .append("requestedOn", requestedOn)
                .append("respondedOn", respondedOn)
                .append("readOn", readOn)
                .append("retryHistoryList", retryHistoryList)
                .append("proxifiable", proxifiable)
                .append("forcedProtocolFamily", forcedProtocolFamily)
                .append("forcedRemoteAddress", forcedRemoteAddress)
                .toString();
    }

}
