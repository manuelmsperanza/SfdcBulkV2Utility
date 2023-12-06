package com.hoffnungland.sfdcBulkV2Utility;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;

public class SfdcHttpClientResponseHandler implements HttpClientResponseHandler<SfdcHttpClientResponse> {

	public SfdcHttpClientResponse sfdcHttpClientResponse;
	
	@Override
	public SfdcHttpClientResponse handleResponse(ClassicHttpResponse response) throws HttpException, IOException {
		
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
            return this.sfdcHttpClientResponse;
		
	}

}
