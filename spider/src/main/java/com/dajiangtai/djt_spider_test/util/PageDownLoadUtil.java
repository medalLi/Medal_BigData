package com.dajiangtai.djt_spider_test.util;

import com.dajiangtai.djt_spider.entity.Page;
import com.dajiangtai.djt_spider.service.impl.HttpClientDownLoadService;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * 页面下载工具类
 * @author dajiangtai
 * created by 2016-10-28
 *
 */
public class PageDownLoadUtil {

	public static String getPageContent(String url){
		HttpClientBuilder builder = HttpClients.custom();
		CloseableHttpClient client = builder.build();
		
		HttpGet request = new HttpGet(url);
		String content = null;
		try {
		    CloseableHttpResponse response = client.execute(request);
			HttpEntity  entity = response.getEntity();
			content = EntityUtils.toString(entity);
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return content;	
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String url = "http://tv.youku.com/";
		/*HttpClientDownLoadService down = new HttpClientDownLoadService();
		Page page = down.download(url);
		System.out.println(page.getContent());*/
		String content = PageDownLoadUtil.getPageContent(url);
		System.out.println(content);
	}

}
