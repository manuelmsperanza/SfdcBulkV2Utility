package com.hoffnungland.sfdcBulkV2Utility;

import java.io.IOException;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.BasicHttpClientResponseHandler;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SfdcApiUtils {
	
	private static final Logger logger = LogManager.getLogger(SfdcApiUtils.class);
	
	public static String checkLimits(String sessionId, String baseUrl, String apiVersion) throws IOException {
		logger.traceEntry();
		String response = null;
		
		try(CloseableHttpClient httpclient = HttpClients.createDefault()){
			
			HttpGet getRequest = new HttpGet(baseUrl + "/services/data/" + apiVersion + "/limits");
			getRequest.addHeader("Authorization", "Bearer " + sessionId);
			
			BasicHttpClientResponseHandler responseClientHandler = new BasicHttpClientResponseHandler();
			response = httpclient.execute(getRequest, responseClientHandler);
			
		}
		
		return logger.traceExit(response);
	}
}
