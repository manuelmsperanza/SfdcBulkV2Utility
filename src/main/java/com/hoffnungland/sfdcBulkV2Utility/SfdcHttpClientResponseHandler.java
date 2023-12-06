package com.hoffnungland.sfdcBulkV2Utility;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

public class SfdcHttpClientResponseHandler implements HttpClientResponseHandler<SfdcHttpClientResponse> {
	
	private static final Logger logger = LogManager.getLogger(SfdcHttpClientResponseHandler.class);
	
	public SfdcHttpClientResponse sfdcHttpClientResponse;
	
	@Override
	public SfdcHttpClientResponse handleResponse(ClassicHttpResponse response) throws HttpException, IOException {
		logger.traceEntry();
		int statusCode = response.getCode();
		
        
            if (statusCode >= 200 && statusCode < 300) {
            	this.sfdcHttpClientResponse = new SfdcHttpClientResponse();
                
            	this.sfdcHttpClientResponse.body = EntityUtils.toString(response.getEntity());
            	this.sfdcHttpClientResponse.headers = response.getHeaders();
            	this.sfdcHttpClientResponse.mapHeaders = new HashMap<String, String>();
            	for(Header curHeader : this.sfdcHttpClientResponse.headers) {
            		this.sfdcHttpClientResponse.mapHeaders.put(curHeader.getName(), curHeader.getValue());
            	}
                
            } else {
                
                throw new SfdcHttpResponseException("HTTP request failed with status code: " + statusCode);
            }
            return logger.traceExit(this.sfdcHttpClientResponse);
		
	}

	@Override
	public String toString() {
		
		
		Gson gson = new Gson();
		
		return gson.toJson(this.sfdcHttpClientResponse);
	}

	
	
}
