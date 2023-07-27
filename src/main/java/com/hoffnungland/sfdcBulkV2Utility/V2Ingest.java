package com.hoffnungland.sfdcBulkV2Utility;

import java.io.IOException;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPatch;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.impl.classic.BasicHttpClientResponseHandler;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;

public class V2Ingest {
	
	public static String checkLimits(String sessionId, String baseUrl, String apiVersion) throws IOException {
		String response = null;
		
		try(CloseableHttpClient httpclient = HttpClients.createDefault()){
			
			HttpGet getRequest = new HttpGet(baseUrl + "/services/data/" + apiVersion + "/limits");
			getRequest.addHeader("Authorization", "Bearer " + sessionId);
			
			BasicHttpClientResponseHandler responseClientHandler = new BasicHttpClientResponseHandler();
			response = httpclient.execute(getRequest, responseClientHandler);
		}
		
		return response;
	}
	
	public static String getQueryResult(String sessionId, String baseUrl, String apiVersion, String jobId, String queryLocator, int maxRecords) throws IOException {
		String response = null;
		
		try(CloseableHttpClient httpclient = HttpClients.createDefault()){
			String parameters = null;
			
			if(queryLocator != null && !queryLocator.isEmpty()) {
				parameters = "locator=" + queryLocator;
			}
			if(maxRecords > 0) {
				parameters = (parameters != null ? parameters + "&" : "") + "maxRecords=" + maxRecords;
			}
			
			HttpGet getRequest = new HttpGet(baseUrl + "/services/data/" + apiVersion + "/jobs/query/" + jobId + "/results" + (parameters == null ? "" : "?" + parameters) );
			getRequest.addHeader("Authorization", "Bearer " + sessionId);
			
			BasicHttpClientResponseHandler responseClientHandler = new BasicHttpClientResponseHandler();
			response = httpclient.execute(getRequest, responseClientHandler);
			
		}
		
		return response;
	}
	
	public static String createJob(String sessionId, String baseUrl, String apiVersion, String objectName, String contentType, String operation, String columnDelimiter) throws IOException {
		String response = null;
		
		try(CloseableHttpClient httpclient = HttpClients.createDefault()){
			
			HttpPost postRequest = new HttpPost(baseUrl + "/services/data/" + apiVersion + "/jobs/ingest");
			postRequest.addHeader("Authorization", "Bearer " + sessionId);
			StringEntity myEntity = new StringEntity("{\"object\" : \"" + objectName + "\", \"contentType\" : \"" + contentType + "\", \"operation\" : \"" + operation + "\", \"columnDelimiter\": \"" + columnDelimiter + "\"}",ContentType.create("application/json", "UTF-8"));
			postRequest.setEntity(myEntity);
			BasicHttpClientResponseHandler responseClientHandler = new BasicHttpClientResponseHandler();
			response = httpclient.execute(postRequest, responseClientHandler);
		}
		
		return response;
		
	}
	
	public static String uploadCsvContent(String sessionId, String baseUrl, String apiVersion, String jodId, String csvContent) throws IOException {
		
		String response = null;
		
		try(CloseableHttpClient httpclient = HttpClients.createDefault()){
			HttpPut putRequest = new HttpPut(baseUrl + "/services/data/" + apiVersion + "/jobs/ingest/" + jodId + "/batches");
			putRequest.addHeader("Authorization", "Bearer " + sessionId);
			StringEntity myEntity = new StringEntity(csvContent,ContentType.create("text/csv", "UTF-8"));
			putRequest.setEntity(myEntity);
			BasicHttpClientResponseHandler responseClientHandler = new BasicHttpClientResponseHandler();
			response = httpclient.execute(putRequest, responseClientHandler);
		}
		
		return response;
	}
	
	public static String changeJobStatus(String sessionId, String baseUrl, String apiVersion, String jodId, String status) throws IOException {
		String response = null;
		
		try(CloseableHttpClient httpclient = HttpClients.createDefault()){
			HttpPatch patchRequest = new HttpPatch(baseUrl + "/services/data/" + apiVersion + "/jobs/ingest/" + jodId);
			patchRequest.addHeader("Authorization", "Bearer " + sessionId);
			StringEntity myEntity = new StringEntity("{\"state\" : \"" + status + "\"}",ContentType.create("application/json", "UTF-8"));
			patchRequest.setEntity(myEntity);
			BasicHttpClientResponseHandler responseClientHandler = new BasicHttpClientResponseHandler();
			response = httpclient.execute(patchRequest, responseClientHandler);
		}
		
		return response;
	}
	
	public static String successfulResults(String sessionId, String baseUrl, String apiVersion, String jodId) throws IOException {
		String response = null;
		
		try(CloseableHttpClient httpclient = HttpClients.createDefault()){
			
			HttpGet getRequest = new HttpGet(baseUrl + "/services/data/" + apiVersion + "/jobs/ingest/" + jodId + "/successfulResults");
			getRequest.addHeader("Authorization", "Bearer " + sessionId);
			
			BasicHttpClientResponseHandler responseClientHandler = new BasicHttpClientResponseHandler();
			response = httpclient.execute(getRequest, responseClientHandler);
		}
		
		return response;
	}
	
	public static String failedResults(String sessionId, String baseUrl, String apiVersion, String jodId) throws IOException {
		String response = null;
		
		try(CloseableHttpClient httpclient = HttpClients.createDefault()){
			
			HttpGet getRequest = new HttpGet(baseUrl + "/services/data/" + apiVersion + "/jobs/ingest/" + jodId + "/failedResults");
			getRequest.addHeader("Authorization", "Bearer " + sessionId);
			
			BasicHttpClientResponseHandler responseClientHandler = new BasicHttpClientResponseHandler();
			response = httpclient.execute(getRequest, responseClientHandler);
		}
		
		return response;
	}
	
	public static String unprocessedrecords(String sessionId, String baseUrl, String apiVersion, String jodId) throws IOException {
		String response = null;
		
		try(CloseableHttpClient httpclient = HttpClients.createDefault()){
			
			HttpGet getRequest = new HttpGet(baseUrl + "/services/data/" + apiVersion + "/jobs/ingest/" + jodId + "/unprocessedrecords");
			getRequest.addHeader("Authorization", "Bearer " + sessionId);
			
			BasicHttpClientResponseHandler responseClientHandler = new BasicHttpClientResponseHandler();
			response = httpclient.execute(getRequest, responseClientHandler);
		}
		
		return response;
	}
	
}
