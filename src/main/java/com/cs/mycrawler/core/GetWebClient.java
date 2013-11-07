package com.cs.mycrawler.core;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.NicelyResynchronizingAjaxController;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethodRetryHandler;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;

import java.io.IOException;

/**
 * User: luo fang
 * Date: 13-10-21 Time: 上午11:59
 * Func: get httpclient and getmethod single instance
 */

public class GetWebClient {

    private static HttpClient httpClient = null;

    private static GetMethod getMethod = null;

    private GetWebClient() {
    }

    public static HttpClient getClientInstance() {
        if (httpClient == null) {
            httpClient = new HttpClient();
        }
        return httpClient;
    }

    public static GetMethod getMethodInstance(String uri) {
        if (getMethod == null) {
             getMethod = new GetMethod(uri);
            getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
        }
        return getMethod;
    }

    public static HtmlPage getPage(String url) throws IOException {

        WebClient webClient = new WebClient(BrowserVersion.FIREFOX_2);
        webClient.setJavaScriptEnabled(true);
        webClient.setAjaxController(new NicelyResynchronizingAjaxController());
        webClient.setThrowExceptionOnScriptError(false);

        HtmlPage hp = (HtmlPage) webClient.getPage(url);
        webClient.setTimeout(3000);
         return hp;
    }
}
