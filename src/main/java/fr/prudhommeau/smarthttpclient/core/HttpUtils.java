package fr.prudhommeau.smarthttpclient.core;

import org.apache.http.HttpHost;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class HttpUtils {

    public static HttpHost uriToHttpHost(String uriToParse) {
        try {
            URI uri = new URI(uriToParse);
            return new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static String readHttpResponseContent(byte[] responseAsByteArray) throws IOException {
        return new String(responseAsByteArray, StandardCharsets.UTF_8);
    }

    public static String trim(String response) throws IOException {
        return response.replaceAll("\\s+", " ").trim().replace("\t", "").replace("\n", "");
    }

    public static <T> List<T> intersection(List<T> list1, List<T> list2) {
        List<T> list = new ArrayList<T>();
        for (T t : list1) {
            if (list2.contains(t)) {
                list.add(t);
            }
        }
        return list;
    }
}
