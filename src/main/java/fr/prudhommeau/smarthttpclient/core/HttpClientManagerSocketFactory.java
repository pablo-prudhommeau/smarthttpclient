package fr.prudhommeau.smarthttpclient.core;

import fr.prudhommeau.smarthttpclient.bean.Proxy;
import org.apache.http.HttpHost;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
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
import java.util.Arrays;

public class HttpClientManagerSocketFactory {

    private static final Logger logger = LoggerFactory.getLogger(HttpClientManagerSSLConnectionSocketFactory.class);

    public static class HttpClientManagerPlainConnectionSocketFactory extends org.apache.http.conn.socket.PlainConnectionSocketFactory {

        @Override
        public Socket createSocket(final HttpContext context) throws IOException {
            if (context.getAttribute(HttpClientManager.HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_SOCKS_PROXY) != null) {
                Proxy socksProxy = (Proxy) context.getAttribute(HttpClientManager.HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_SOCKS_PROXY);
                InetSocketAddress inetSocketAddress = new InetSocketAddress(socksProxy.getIp(), Integer.parseInt(socksProxy.getPort()));
                java.net.Proxy proxy = new java.net.Proxy(java.net.Proxy.Type.SOCKS, inetSocketAddress);
                return new Socket(proxy);
            }
            return super.createSocket(context);
        }

        @Override
        public Socket connectSocket(int connectTimeout, Socket socket, HttpHost host, InetSocketAddress remoteAddress, InetSocketAddress localAddress, HttpContext context) throws IOException {
            StandardProtocolFamily standardProtocolFamily = (StandardProtocolFamily) context.getAttribute(HttpClientManager.HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_FORCE_PROTOCOL_FAMILY);
            Proxy httpProxy = (Proxy) context.getAttribute(HttpClientManager.HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_HTTP_PROXY);
            Proxy socksProxy = (Proxy) context.getAttribute(HttpClientManager.HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_SOCKS_PROXY);
            if (standardProtocolFamily != null) {
                if (httpProxy != null) {
                    logger.warn("An attempt of rewriting protocol family to [" + standardProtocolFamily + "] was made but an HTTP proxy [" + httpProxy + "] has also been configured. Ignoring standard protocol family rewrite... ");
                    return super.connectSocket(connectTimeout, socket, host, remoteAddress, localAddress, context);
                } else if (socksProxy != null) {
                    logger.debug("An attempt of rewriting protocol family to [" + standardProtocolFamily + "] was made but a SOCKS proxy [" + socksProxy + "] has also been configured. Ignoring standard protocol family rewrite... ");
                    return super.connectSocket(connectTimeout, socket, host, remoteAddress, localAddress, context);
                }
                HttpRoute httpRoute = (HttpRoute) context.getAttribute(HttpClientManager.HTTP_CONTEXT_ATTRIBUTE_STANDARD_KEY_HTTP_ROUTE);
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

    }

    public static class HttpClientManagerSSLConnectionSocketFactory extends SSLConnectionSocketFactory {

        public HttpClientManagerSSLConnectionSocketFactory() {
            super(SSLContexts.createSystemDefault(), NoopHostnameVerifier.INSTANCE);
        }

        @Override
        public Socket createSocket(final HttpContext context) throws IOException {
            if (context.getAttribute(HttpClientManager.HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_SOCKS_PROXY) != null) {
                Proxy socksProxy = (Proxy) context.getAttribute(HttpClientManager.HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_SOCKS_PROXY);
                InetSocketAddress inetSocketAddress = new InetSocketAddress(socksProxy.getIp(), Integer.parseInt(socksProxy.getPort()));
                java.net.Proxy proxy = new java.net.Proxy(java.net.Proxy.Type.SOCKS, inetSocketAddress);
                return new Socket(proxy);
            }
            return super.createSocket(context);
        }

        @Override
        public Socket connectSocket(int connectTimeout, Socket socket, HttpHost host, InetSocketAddress remoteAddress, InetSocketAddress localAddress, HttpContext context) throws IOException {
            StandardProtocolFamily standardProtocolFamily = (StandardProtocolFamily) context.getAttribute(HttpClientManager.HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_FORCE_PROTOCOL_FAMILY);
            Proxy httpProxy = (Proxy) context.getAttribute(HttpClientManager.HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_HTTP_PROXY);
            Proxy socksProxy = (Proxy) context.getAttribute(HttpClientManager.HTTP_CONTEXT_ATTRIBUTE_CUSTOM_KEY_SOCKS_PROXY);
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

    }

}
