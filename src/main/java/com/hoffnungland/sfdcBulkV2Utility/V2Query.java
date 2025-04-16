package com.hoffnungland.sfdcBulkV2Utility;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.BasicHttpClientResponseHandler;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class V2Query {
	
	private static final Logger logger = LogManager.getLogger(V2Query.class);

	public static JsonObject writeJsonProps(String fileName, String jobInfo) throws IOException {
		
		logger.traceEntry();
		
		File jsonFile = new File(fileName);
		Gson gson = new GsonBuilder().setPrettyPrinting().create();

		JsonObject jsonObject = null;
		JsonObject innerObject = null;

		if (jsonFile.exists()) {

			try (FileReader fr = new FileReader(jsonFile)) {
				JsonElement jsonElement = JsonParser.parseReader(fr);
				jsonObject = jsonElement.getAsJsonObject();
				//innerObject = jsonObject.getAsJsonObject("query");
			}
		} else {
			jsonObject = new JsonObject();
			//innerObject = new JsonObject();
			//jsonObject.add("query", innerObject);
		}

		if(jsonObject.has("query")) {					
			innerObject = jsonObject.getAsJsonObject("query");
		} else {
			innerObject = new JsonObject();
			jsonObject.add("query", innerObject);
		}
		
		JsonElement jsonElement = JsonParser.parseString(jobInfo);
		JsonObject srcJsonObject = jsonElement.getAsJsonObject();
		for(Entry<String, JsonElement> curEntry : srcJsonObject.entrySet()) {
			innerObject.add(curEntry.getKey(), curEntry.getValue());
		}
		
		try (FileWriter fw = new FileWriter(jsonFile)) {
			gson.toJson(jsonObject, fw);
		}
		
		return logger.traceExit(jsonObject);
	}

	public static String createJob(String sessionId, String baseUrl, String apiVersion, String operation, String query, String columnDelimiter) throws IOException {
		logger.traceEntry();
		String response = null;

		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {

			HttpPost postRequest = new HttpPost(baseUrl + "/services/data/" + apiVersion + "/jobs/query");
			postRequest.addHeader("Authorization", "Bearer " + sessionId);
			StringEntity myEntity = new StringEntity("{\"operation\" : \"" + operation + "\", \"query\": \"" + query
					+ "\", \"columnDelimiter\": \"" + columnDelimiter + "\"}",
					ContentType.create("application/json", "UTF-8"));
			postRequest.setEntity(myEntity);
			BasicHttpClientResponseHandler responseClientHandler = new BasicHttpClientResponseHandler();
			response = httpclient.execute(postRequest, responseClientHandler);
		}

		return logger.traceExit(response);

	}
	
	public static String getJobsInfo(String sessionId, String baseUrl, String apiVersion, String nextRecordsUrl) throws IOException {
		
		logger.traceEntry();
		String response = null;

		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {

			HttpGet getRequest = new HttpGet(baseUrl + (nextRecordsUrl == null ? "/services/data/" + apiVersion + "/jobs/query" : nextRecordsUrl));
			getRequest.addHeader("Authorization", "Bearer " + sessionId);

			BasicHttpClientResponseHandler responseClientHandler = new BasicHttpClientResponseHandler();
			response = httpclient.execute(getRequest, responseClientHandler);
		}

		return logger.traceExit(response);
	}
	
	public static String getJobInfo(String sessionId, String baseUrl, String apiVersion, String jodId) throws IOException {
		
		logger.traceEntry();
		String response = null;

		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {

			HttpGet getRequest = new HttpGet(baseUrl + "/services/data/" + apiVersion + "/jobs/query/" + jodId);
			getRequest.addHeader("Authorization", "Bearer " + sessionId);

			BasicHttpClientResponseHandler responseClientHandler = new BasicHttpClientResponseHandler();
			response = httpclient.execute(getRequest, responseClientHandler);
		}

		return logger.traceExit(response);
	}

	public static SfdcHttpClientResponse getQueryResult(String sessionId, String baseUrl, String apiVersion, String jobId, String queryLocator, int maxRecords) throws IOException {
		
		logger.traceEntry();
		
		SfdcHttpClientResponse response = null;

		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			String parameters = null;

			if (queryLocator != null && !queryLocator.isEmpty()) {
				parameters = "locator=" + queryLocator;
			}
			if (maxRecords > 0) {
				parameters = (parameters != null ? parameters + "&" : "") + "maxRecords=" + maxRecords;
			}

			HttpGet getRequest = new HttpGet(baseUrl + "/services/data/" + apiVersion + "/jobs/query/" + jobId
					+ "/results" + (parameters == null ? "" : "?" + parameters));
			getRequest.addHeader("Authorization", "Bearer " + sessionId);

			SfdcHttpClientResponseHandler responseClientHandler = new SfdcHttpClientResponseHandler();
			response = httpclient.execute(getRequest, responseClientHandler);

		}

		return logger.traceExit(response);
	}

	public static void extractV2Query(String sessionId, String baseUrl, String apiVersion, String jobName, int sleepTime, String operation, String query, String columnDelimiter, String tmpDir, String outputDir, String archiveFilenamePrefix, String delTmpFilenamePrefix) throws FileNotFoundException, IOException, InterruptedException {
		logger.traceEntry();
		scheduleV2Query(sessionId, baseUrl, apiVersion, jobName, operation, query, columnDelimiter, tmpDir, sleepTime);
		bulkV2Query(sessionId, baseUrl, apiVersion, jobName, columnDelimiter, tmpDir, outputDir, archiveFilenamePrefix, delTmpFilenamePrefix);
		logger.traceExit();
	}

	public static void scheduleV2Query(String sessionId, String baseUrl, String apiVersion, String jobName, String operation, String query, String columnDelimiter, String tmpDir, int sleepTime) throws FileNotFoundException, IOException, InterruptedException {
		logger.traceEntry();
		launchV2Query(sessionId, baseUrl, apiVersion, jobName, operation, query, columnDelimiter, tmpDir);
		waitV2QueryCompletion(sessionId, baseUrl, apiVersion, jobName, tmpDir, sleepTime);
		logger.traceExit();
	}

	public static void launchV2Query(String sessionId, String baseUrl, String apiVersion, String jobName, String operation, String query,String columnDelimiter, String tmpDir) throws FileNotFoundException, IOException {
		logger.traceEntry();
		String fileName = tmpDir + jobName + ".json";
		File jsonFile = new File(fileName);

		boolean skipLaunchV2Query = jsonFile.exists();
		if (skipLaunchV2Query) {
			try (FileReader fr = new FileReader(jsonFile)) {
				
				JsonObject jsonObject = null;
				JsonObject innerObject = null;

				JsonElement jsonElement = JsonParser.parseReader(fr);
				jsonObject = jsonElement.getAsJsonObject();
				if(skipLaunchV2Query = jsonObject.has("query")) {					
					innerObject = jsonObject.getAsJsonObject("query");
					if (skipLaunchV2Query = innerObject.has("id")) {
						skipLaunchV2Query = (innerObject.getAsJsonPrimitive("id") != null);
					}
				}
			}
		}
		
		if(skipLaunchV2Query) {
			logger.warn("skip");
			logger.traceExit();
			return;
		}
		
		String jobResponse = createJob(sessionId, baseUrl, apiVersion, operation, query, columnDelimiter);
		
		writeJsonProps(fileName, jobResponse);
		
		logger.traceExit();

	}
	
	public static void waitV2QueryCompletion(String sessionId, String baseUrl, String apiVersion, String jobName, String tmpDir, int sleepTime) throws FileNotFoundException, IOException, InterruptedException {
		logger.traceEntry();
		String fileName = tmpDir + jobName + ".json";
		File jsonFile = new File(fileName);
		
		boolean skipWaitV2QueryCompletion = !jsonFile.exists();
		String jobId = null;
		if (skipWaitV2QueryCompletion) {
			logger.error(fileName + "does not exist. Job must be created");
		} else {
			try (FileReader fr = new FileReader(jsonFile)) {
				
				JsonObject jsonObject = null;
				JsonObject innerObject = null;

				JsonElement jsonElement = JsonParser.parseReader(fr);
				jsonObject = jsonElement.getAsJsonObject();
				innerObject = jsonObject.getAsJsonObject("query");
				JsonElement jobIdEl = innerObject.get("id");
				skipWaitV2QueryCompletion = jobIdEl == null || (jobId = jobIdEl.getAsString()).isEmpty(); 
				if(skipWaitV2QueryCompletion) {
					logger.error("Job id does not exists");
				} else {
					skipWaitV2QueryCompletion = innerObject.has("state");
					if (skipWaitV2QueryCompletion) {
						switch (innerObject.get("state").getAsString()) {
						case "JobComplete":
						case "Aborted":
						case "Failed":
							skipWaitV2QueryCompletion = true;
							break;
						default:
							skipWaitV2QueryCompletion = false;
						}				
					}
				}
			}
		}
		
		if(skipWaitV2QueryCompletion) {
			logger.warn("skip");
			logger.traceExit();
			return;
		}
		skipWaitV2QueryCompletion = false;
		do {

			String jobResponse = getJobInfo(sessionId, baseUrl, apiVersion, jobId);

			JsonObject jsonObject = writeJsonProps(fileName, jobResponse);
			JsonObject innerObject = jsonObject.getAsJsonObject("query");

			switch (innerObject.get("state").getAsString()) {
			case "InProgress":
			case "UploadComplete":
				skipWaitV2QueryCompletion = false;
				logger.info("Sleep " + sleepTime + " [s]");
				Thread.sleep(sleepTime*1000);
				break;
			default:
				skipWaitV2QueryCompletion = true;
			}
		} while (!skipWaitV2QueryCompletion);

		logger.traceExit();

	}
	
	public static void bulkV2Query(String sessionId, String baseUrl, String apiVersion, String jobName, String columnDelimiter, String tmpDir, String outputDir, String archiveFilenamePrefix, String delTmpFilenamePrefix) throws FileNotFoundException, IOException {
		
		logger.traceEntry();
		String fileName = tmpDir + jobName + ".json";
		File jsonFile = new File(fileName);
		
		boolean skipBulkV2Query = !jsonFile.exists();
		String jobId = null;
		String requestLocator = null;
		if (skipBulkV2Query) {
			logger.error(fileName + "does not exist. Job must be created");
		} else {
			try (FileReader fr = new FileReader(jsonFile)) {
				
				JsonObject jsonObject = null;
				JsonObject innerObject = null;

				JsonElement jsonElement = JsonParser.parseReader(fr);
				jsonObject = jsonElement.getAsJsonObject();
				innerObject = jsonObject.getAsJsonObject("query");
				JsonElement jobIdEl = innerObject.get("id");
				skipBulkV2Query = jobIdEl == null || (jobId = jobIdEl.getAsString()).isEmpty(); 
				if(skipBulkV2Query) {
					logger.error("Job id does not exists");
				} else {
					skipBulkV2Query = !innerObject.has("state");
					if (skipBulkV2Query) {
						logger.error("Job status does not exists");
					} else {
						String jobState = innerObject.get("state").getAsString();
						switch (jobState) {
						case "JobComplete":
							if(innerObject.has("requestLocator")) {
								requestLocator = innerObject.get("requestLocator").getAsString();
							}
							skipBulkV2Query = "null".equals(requestLocator);
							break;
						case "Aborted":
						case "Failed":
							skipBulkV2Query = true;
							break;
						default:
							logger.error("Job status is not valid: " + jobState);
							skipBulkV2Query = true;
						}
					}
				}
			}
		}
		
		if(skipBulkV2Query) {
			logger.warn("skip");
			logger.traceExit();
			return;
		}
		
		long totalRecord = 0;
		CsvArchiver csvArchiver = new CsvArchiver();
		
		char retrieveDelimiterChar = ',';
		char archiveDelimiterChar = ',';
		switch (columnDelimiter) {
		case "BACKQUOTE":
			retrieveDelimiterChar = '`';
			archiveDelimiterChar = '`';
			break;
		case "CARET":
			retrieveDelimiterChar = '^';
			archiveDelimiterChar = '^';
			break;
		case "COMMA":
			retrieveDelimiterChar = ',';
			archiveDelimiterChar = ',';
			break;
		case "PIPE":
			retrieveDelimiterChar = '|';
			archiveDelimiterChar = '|';
			break;
		case "SEMICOLON":
			retrieveDelimiterChar = ';';
			archiveDelimiterChar = ';';
			break;
		case "TAB":
			retrieveDelimiterChar = '\t';
			archiveDelimiterChar = '\t';
			break;
		}
		
		csvArchiver.initialize(outputDir, archiveFilenamePrefix, tmpDir + delTmpFilenamePrefix, retrieveDelimiterChar, archiveDelimiterChar);
		int loopIdx = 0;
		skipBulkV2Query = false;
		do {
			logger.trace("Get request " + requestLocator);
			SfdcHttpClientResponse jobResponse = getQueryResult(sessionId, baseUrl, apiVersion, jobId, requestLocator, 0);
			
			requestLocator = jobResponse.mapHeaders.get("Sforce-Locator");
			String numberofRows = jobResponse.mapHeaders.get("Sforce-NumberOfRecords");
			totalRecord += Integer.valueOf(numberofRows == null ? "0" : numberofRows);
			logger.info("Sforce-Locator: " + requestLocator + " Sforce-NumberOfRecords: " + numberofRows + " total: " + totalRecord);
			
			csvArchiver.parseResponse(jobResponse.body, loopIdx++);
			
			writeJsonProps(fileName, "{\"requestLocator\" : \"" + requestLocator + "\"}");

			if(requestLocator == null || "".equals(requestLocator) || "null".equals(requestLocator)){
				skipBulkV2Query = true;
				csvArchiver.close();
			}
			
		} while(!skipBulkV2Query);
		
		logger.traceExit();
	}
	
}
