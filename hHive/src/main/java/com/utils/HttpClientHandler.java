package com.utils;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

/**
 * HttpClient 辅助类.
 * 将HttpClient发送Post、Get请求进行了封装.
 * 调用时只需要设置URL已经请求参数即可.
 */
public class HttpClientHandler {

    public static int socketTimeout = 50000;
    public static int connectTimeout = 50000;
    public static int connectionRequestTimeout = 50000;
    public static String encoding = "UTF-8";
    private CloseableHttpClient client = null;
    public static Map<String, String> headerParams = new HashMap<String, String>();

    /**
     * 初始header
     */
    public HttpClientHandler() {
        /*headerParams.put("Content-type", "application/json; charset=utf-8");
        headerParams.put("connection", "Keep-Alive");
        headerParams.put("User-Agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");*/
    }

    public static HttpClientHandler getHttpClientHandler() {
        return new HttpClientHandler();
    }

    /**
     * 执行Post请求.
     * 无header参数
     * @param url
     * @param params
     */
    public String doPost(String url, Map<String, String> params) throws IOException {
        return doPost(url, headerParams, params);
    }

    /**
     * 执行Post请求.
     * 有header参数
     * @param url
     * @param params
     */
    public String doPost(String url, Map<String, String> headers, Map<String, String> params) throws IOException {
        String result = "";
        getHttpClient();
        HttpPost httpPost = new HttpPost(url);
        for (Iterator<String> it = headers.keySet().iterator(); it.hasNext(); ) {
            String key = it.next();
            String Value = headers.get(key);
            headerParams.put(key, Value);
        }
        for (Iterator<String> it = headerParams.keySet().iterator(); it.hasNext(); ) {
            String key = it.next();
            String Value = headerParams.get(key);
            httpPost.setHeader(key, Value);
        }
        List<NameValuePair> nameValuePair = new ArrayList<NameValuePair>();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            nameValuePair.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
        }
      /*  for (Iterator<String> it = params.keySet().iterator(); it.hasNext(); ) {
            String key = it.next();
            nameValuePair.add(new BasicNameValuePair(key, params.get(key)));
        }*/
        httpPost.setEntity(new UrlEncodedFormEntity(nameValuePair, "utf-8"));
        CloseableHttpResponse response = client.execute(httpPost);
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            result = EntityUtils.toString(entity, encoding);
        }
        EntityUtils.consume(entity);
        response.close();
        close();
        return result;
    }

    /**
     * 执行Post请求.
     * 参数字符串
     * @param url
     * @param params
     */
    public String doPost(String url, Map<String, String> headers, String params) throws IOException {
        String result = "";
        getHttpClient();
        HttpPost httpPost = new HttpPost(url);
        for (Iterator<String> it = headers.keySet().iterator(); it.hasNext(); ) {
            String key = it.next();
            String Value = headers.get(key);
            headerParams.put(key, Value);
        }
        for (Iterator<String> it = headerParams.keySet().iterator(); it.hasNext(); ) {
            String key = it.next();
            String Value = headerParams.get(key);
            httpPost.setHeader(key, Value);
        }
        StringEntity stringEntity = new StringEntity(params, Charset.forName("UTF-8"));
        httpPost.setEntity(stringEntity);
        CloseableHttpResponse response = client.execute(httpPost);
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            result = EntityUtils.toString(entity, encoding);
        }
        EntityUtils.consume(entity);
        response.close();
        close();
        return result;
    }

    /**
     * 执行GET请求.
     * 无header参数
     * @param url
     */
    public String doGet(String url) throws IOException {
        return doGet(url, headerParams);
    }

    /**
     * 执行GET请求.
     * 有headler参数
     * @param url
     * @return
     * @throws IOException
     */
    public String doGet(String url, Map<String,String> headers) throws IOException {
        String result = "";
        getHttpClient();
        HttpGet get = new HttpGet(url);
        for (Iterator<String> it = headers.keySet().iterator(); it.hasNext(); ) {
            String key = it.next();
            String Value = headers.get(key);
            headerParams.put(key, Value);
        }
        for (Iterator<String> it = headerParams.keySet().iterator(); it.hasNext(); ) {
            String key = it.next();
            String Value = headerParams.get(key);
            get.setHeader(key,Value);
        }
        CloseableHttpResponse response = client.execute(get);
        try {
            // 获取响应实体
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                result = EntityUtils.toString(entity, encoding);
            }
        } finally {
            response.close();
        }
        return result;
    }


    /**
     * 获取client.
     */
    public void getHttpClient() {
        if (this.client == null) {
            RequestConfig defaultRequestConfig = RequestConfig.custom()
                    .setSocketTimeout(socketTimeout)
                    .setConnectTimeout(connectTimeout)
                    .setConnectionRequestTimeout(connectionRequestTimeout)
                    .build();
            this.client = HttpClients.custom().setDefaultRequestConfig(defaultRequestConfig).build();
        }

    }

    /**
     * 关闭client.
     *
     * @throws IOException
     */
    public void close() throws IOException {
        if (this.client != null) {
            this.client.close();
        }
    }
}
