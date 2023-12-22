package com.hoffnungland.sfdcBulkV2Utility;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPatch;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
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

public class V2Ingest {
	
	private static final Logger logger = LogManager.getLogger(V2Ingest.class);
	
	public static JsonObject writeJsonProps(String fileName, String jobInfo) throws IOException {
		
		logger.traceEntry();
		
		File jsonFile = new File(fileName);
		Gson gson = new GsonBuilder().setPrettyPrinting().create();

		JsonObject jsonObject = null;
		JsonObject innerObject = null;
		JsonObject jobObject = null;

		if (jsonFile.exists()) {

			try (FileReader fr = new FileReader(jsonFile)) {
				JsonElement jsonElement = JsonParser.parseReader(fr);
				jsonObject = jsonElement.getAsJsonObject();
				//innerObject = jsonObject.getAsJsonObject("ingest");
			}
		} else {
			jsonObject = new JsonObject();
			//innerObject = new JsonObject();
			//jsonObject.add("ingest", innerObject);
		}
		
		if(jsonObject.has("query")) {					
			innerObject = jsonObject.getAsJsonObject("query");
		} else {
			innerObject = new JsonObject();
			jsonObject.add("query", innerObject);
		}

		JsonElement jsonElement = JsonParser.parseString(jobInfo);
		JsonObject srcJsonObject = jsonElement.getAsJsonObject();
		
		String jobId = srcJsonObject.get("id").getAsString();
		
		innerObject.addProperty("lastJobId", jobId);
		
		if(innerObject.has(jobId)) {
			jobObject = innerObject.getAsJsonObject(jobId);
		} else {
			jobObject = new JsonObject();
			innerObject.add(jobId, jobObject);
		}
		
		for(Entry<String, JsonElement> curEntry : srcJsonObject.entrySet()) {
			jobObject.add(curEntry.getKey(), curEntry.getValue());
		}
		
		try (FileWriter fw = new FileWriter(jsonFile)) {
			gson.toJson(jsonObject, fw);
		}
		
		return logger.traceExit(jsonObject);
	}
	
	
	public static String createJob(String sessionId, String baseUrl, String apiVersion, String objectName, String contentType, String operation, String columnDelimiter) throws IOException {
		logger.traceEntry();
		String response = null;
		
		try(CloseableHttpClient httpclient = HttpClients.createDefault()){
			
			HttpPost postRequest = new HttpPost(baseUrl + "/services/data/" + apiVersion + "/jobs/ingest");
			postRequest.addHeader("Authorization", "Bearer " + sessionId);
			StringEntity myEntity = new StringEntity("{\"object\" : \"" + objectName + "\", \"contentType\" : \"" + contentType + "\", \"operation\" : \"" + operation + "\", \"columnDelimiter\": \"" + columnDelimiter + "\"}",ContentType.create("application/json", "UTF-8"));
			postRequest.setEntity(myEntity);
			BasicHttpClientResponseHandler responseClientHandler = new BasicHttpClientResponseHandler();
			response = httpclient.execute(postRequest, responseClientHandler);
		}
		
		return logger.traceExit(response);
		
	}
	
	public static String uploadCsvContent(String sessionId, String baseUrl, String apiVersion, String jodId, String csvContent) throws IOException {
		logger.traceEntry();
		String response = null;
		
		try(CloseableHttpClient httpclient = HttpClients.createDefault()){
			HttpPut putRequest = new HttpPut(baseUrl + "/services/data/" + apiVersion + "/jobs/ingest/" + jodId + "/batches");
			putRequest.addHeader("Authorization", "Bearer " + sessionId);
			StringEntity myEntity = new StringEntity(csvContent,ContentType.create("text/csv", "UTF-8"));
			putRequest.setEntity(myEntity);
			BasicHttpClientResponseHandler responseClientHandler = new BasicHttpClientResponseHandler();
			response = httpclient.execute(putRequest, responseClientHandler);
		}
		
		return logger.traceExit(response);
	}
	
	public static String getJobInfo(String sessionId, String baseUrl, String apiVersion, String jodId) throws IOException {
		logger.traceEntry();
		String response = null;
		
		try(CloseableHttpClient httpclient = HttpClients.createDefault()){
			
			HttpGet getRequest = new HttpGet(baseUrl + "/services/data/" + apiVersion + "/jobs/ingest/" + jodId);
			getRequest.addHeader("Authorization", "Bearer " + sessionId);
			
			BasicHttpClientResponseHandler responseClientHandler = new BasicHttpClientResponseHandler();
			response = httpclient.execute(getRequest, responseClientHandler);
		}
		
		return logger.traceExit(response);
	}
	
	public static String changeJobStatus(String sessionId, String baseUrl, String apiVersion, String jodId, String status) throws IOException {
		logger.traceEntry();
		String response = null;
		
		try(CloseableHttpClient httpclient = HttpClients.createDefault()){
			HttpPatch patchRequest = new HttpPatch(baseUrl + "/services/data/" + apiVersion + "/jobs/ingest/" + jodId);
			patchRequest.addHeader("Authorization", "Bearer " + sessionId);
			StringEntity myEntity = new StringEntity("{\"state\" : \"" + status + "\"}",ContentType.create("application/json", "UTF-8"));
			patchRequest.setEntity(myEntity);
			BasicHttpClientResponseHandler responseClientHandler = new BasicHttpClientResponseHandler();
			response = httpclient.execute(patchRequest, responseClientHandler);
		}
		
		return logger.traceExit(response);
	}
	
	public static String successfulResults(String sessionId, String baseUrl, String apiVersion, String jodId) throws IOException {
		logger.traceEntry();
		String response = null;
		
		try(CloseableHttpClient httpclient = HttpClients.createDefault()){
			
			HttpGet getRequest = new HttpGet(baseUrl + "/services/data/" + apiVersion + "/jobs/ingest/" + jodId + "/successfulResults");
			getRequest.addHeader("Authorization", "Bearer " + sessionId);
			
			BasicHttpClientResponseHandler responseClientHandler = new BasicHttpClientResponseHandler();
			response = httpclient.execute(getRequest, responseClientHandler);
		}
		
		return logger.traceExit(response);
	}
	
	public static String failedResults(String sessionId, String baseUrl, String apiVersion, String jodId) throws IOException {
		logger.traceEntry();
		String response = null;
		
		try(CloseableHttpClient httpclient = HttpClients.createDefault()){
			
			HttpGet getRequest = new HttpGet(baseUrl + "/services/data/" + apiVersion + "/jobs/ingest/" + jodId + "/failedResults");
			getRequest.addHeader("Authorization", "Bearer " + sessionId);
			
			BasicHttpClientResponseHandler responseClientHandler = new BasicHttpClientResponseHandler();
			response = httpclient.execute(getRequest, responseClientHandler);
		}
		
		return logger.traceExit(response);
	}
	
	public static String unprocessedrecords(String sessionId, String baseUrl, String apiVersion, String jodId) throws IOException {
		logger.traceEntry();
		String response = null;
		
		try(CloseableHttpClient httpclient = HttpClients.createDefault()){
			
			HttpGet getRequest = new HttpGet(baseUrl + "/services/data/" + apiVersion + "/jobs/ingest/" + jodId + "/unprocessedrecords");
			getRequest.addHeader("Authorization", "Bearer " + sessionId);
			
			BasicHttpClientResponseHandler responseClientHandler = new BasicHttpClientResponseHandler();
			response = httpclient.execute(getRequest, responseClientHandler);
		}
		
		return logger.traceExit(response);
	}
	
	public static void bulkV2Ingest(String sessionId, String baseUrl, String apiVersion, String jobName, int sleepTime, String objectName, String contentType, String operation, String columnDelimiter, String inputFilePath, String tmpDir, String outputDir) throws IOException, InterruptedException {
		logger.traceEntry();
		scheduleV2Ingest(sessionId, baseUrl, apiVersion, jobName, objectName, contentType, operation, columnDelimiter, inputFilePath, tmpDir, outputDir);
		waitV2IngestCompletion(sessionId, baseUrl, apiVersion, jobName, tmpDir, sleepTime);
		getV2IngestResult(sessionId, baseUrl, apiVersion, jobName, tmpDir, outputDir);
		logger.traceExit();
	}
	
	public static void scheduleV2Ingest(String sessionId, String baseUrl, String apiVersion, String jobName, String objectName, String contentType, String operation, String columnDelimiter, String inputFilePath, String tmpDir, String outputDir) throws IOException, InterruptedException {
		
		logger.traceEntry();
		
		String fileName = tmpDir + jobName + ".json";
		File jsonFile = new File(fileName);
		
		boolean skipBulkV2Ingest = jsonFile.exists();
		
		if (skipBulkV2Ingest) {
			try (FileReader fr = new FileReader(jsonFile)) {
				
				JsonObject jsonObject = null;
				JsonObject innerObject = null;

				JsonElement jsonElement = JsonParser.parseReader(fr);
				jsonObject = jsonElement.getAsJsonObject();
				if(skipBulkV2Ingest = jsonObject.has("ingest")) {					
					innerObject = jsonObject.getAsJsonObject("ingest");
					if (skipBulkV2Ingest = innerObject.has("lastJobId")) {
						skipBulkV2Ingest = (innerObject.getAsJsonPrimitive("lastJobId") != null);
					}
				}
			}
		}
		
		if(skipBulkV2Ingest) {
					
			logger.warn("skip");
			logger.traceExit();
			return;
		}
		
		char columnDelimiterChar = ',';
		switch (columnDelimiter) {
		case "BACKQUOTE":
			columnDelimiterChar = '`';
			break;
		case "CARET":
			columnDelimiterChar = '^';
			break;
		case "COMMA":
			columnDelimiterChar = ',';
			break;
		case "PIPE":
			columnDelimiterChar = '|';
			break;
		case "SEMICOLON":
			columnDelimiterChar = ';';
			break;
		case "TAB":
			columnDelimiterChar = '\t';
			break;

		}
		
		CSVFormat csvFormat = CSVFormat.Builder.create().setQuoteMode(QuoteMode.ALL).setDelimiter(columnDelimiterChar).build();
		
		File inputFile = new File(inputFilePath);
		CSVParser csvParser = CSVParser.parse(inputFile, StandardCharsets.UTF_8, csvFormat);		
		StringBuilder ingestPayload = new StringBuilder();
		
		String header = null;
		long headerSize = 0;
		long stringSize = 0;
		//int fileCount = 0;
		
		Iterator<CSVRecord> csvRecordIterator = csvParser.iterator();
		while(csvRecordIterator.hasNext()) {
			
			CSVRecord curRecord = csvRecordIterator.next();
			logger.debug(curRecord);
		
			String line = csvFormat.format(curRecord.values()) + "\n";
			
			if(header == null) {
				header = line;
				if(header.length() > 32000) {
					logger.error("Header too long");
					logger.traceExit();
					return;
				}
				ingestPayload.append(line);
				headerSize = (new Double(Math.ceil(header.getBytes().length/3)*4)).longValue();
				stringSize = headerSize;
				logger.trace(headerSize + " " + header);
			} else {
				
				long encodedSize = (new Double(Math.ceil(line.getBytes().length/3)*4)).longValue();
				if(stringSize + encodedSize > 150*1024*1024){
					logger.info("flush size " + stringSize);
					String csvContent = ingestPayload.toString();
					/*try(BufferedWriter writer = new BufferedWriter(new FileWriter(outputDir + jobName + "_" + fileCount++ + ".csv"))){
						
						writer.write(csvContent);
					}*/
					
					ingestPayload = new StringBuilder(header);
					stringSize = headerSize;
					startIngestJob(sessionId, baseUrl, apiVersion, fileName, objectName, contentType, operation, columnDelimiter, csvContent);

				}
				ingestPayload.append(line);
				stringSize += encodedSize;
				logger.debug(stringSize + " " + line);

				
			}
		}
		
		logger.info("flush total record " + stringSize);
		String csvContent = ingestPayload.toString();
		/*try(BufferedWriter writer = new BufferedWriter(new FileWriter(outputDir + jobName + "_" + fileCount++ + ".csv"))){
			
			writer.write(csvContent);
		}*/
		startIngestJob(sessionId, baseUrl, apiVersion, fileName, objectName, contentType, operation, columnDelimiter, csvContent);
		
		logger.traceExit();
		
	}
	
	public static void startIngestJob(String sessionId, String baseUrl, String apiVersion, String fileName, String objectName, String contentType, String operation, String columnDelimiter, String csvContent) throws IOException {
		
		logger.traceEntry();
			
		String jobResponse = createJob(sessionId, baseUrl, apiVersion, objectName, contentType, operation, columnDelimiter);
		JsonObject jsonObject = writeJsonProps(fileName, jobResponse);
		JsonObject innerObject = jsonObject.getAsJsonObject("ingest");
		String jobId = innerObject.get("lastJobId").getAsString();
		
		jobResponse = V2Ingest.uploadCsvContent(sessionId, baseUrl, apiVersion, jobId, csvContent);
		//writeJsonProps(fileName, jobResponse);
		jobResponse = V2Ingest.changeJobStatus(sessionId, baseUrl, apiVersion, jobId, "UploadComplete");
		writeJsonProps(fileName, jobResponse);
		
		logger.traceExit();
		
	}
	
	public static void waitV2IngestCompletion(String sessionId, String baseUrl, String apiVersion, String jobName, String tmpDir, int sleepTime) throws FileNotFoundException, IOException, InterruptedException {
		logger.traceEntry();
		String fileName = tmpDir + jobName + ".json";
		File jsonFile = new File(fileName);
		
		boolean skipWaitV2IngestCompletion = !jsonFile.exists();
		
		if (skipWaitV2IngestCompletion) {
			logger.error(fileName + "does not exist. Job must be created");
		} else {
			try (FileReader fr = new FileReader(jsonFile)) {
				
				JsonObject jsonObject = null;
				JsonObject innerObject = null;

				JsonElement jsonElement = JsonParser.parseReader(fr);
				jsonObject = jsonElement.getAsJsonObject();
				innerObject = jsonObject.getAsJsonObject("ingest");
				JsonElement jobIdEl = innerObject.get("lastJobId");
				skipWaitV2IngestCompletion = jobIdEl == null || jobIdEl.getAsString().isEmpty(); 
				if(skipWaitV2IngestCompletion) {
					logger.error("Job id does not exists");
				}
			}
		}
		
		if(skipWaitV2IngestCompletion) {
			logger.warn("skip");
			logger.traceExit();
			return;
		}
		
		do {
			
			skipWaitV2IngestCompletion = true;
			
			JsonObject jsonObject = null;
			JsonObject innerObject = null;
			try (FileReader fr = new FileReader(jsonFile)) {
				
				JsonElement jsonElement = JsonParser.parseReader(fr);
				jsonObject = jsonElement.getAsJsonObject();
			}
			innerObject = jsonObject.getAsJsonObject("ingest");
			
			for(Entry<String, JsonElement> curEntry : innerObject.entrySet()) {
				
				JsonElement ingestElement = curEntry.getValue();
				if(ingestElement.isJsonObject()) {
									
					JsonObject ingestObject = ingestElement.getAsJsonObject();
					if(ingestObject.has("id")) {
						
						if (ingestObject.has("state")) {
							switch (ingestObject.get("state").getAsString()) {
							case "Open":
							case "JobComplete":
							case "Aborted":
							case "Failed":
								break;
							default:
								String jobId = ingestObject.get("id").getAsString();
								String jobResponse = getJobInfo(sessionId, baseUrl, apiVersion, jobId);
								writeJsonProps(fileName, jobResponse);
								skipWaitV2IngestCompletion = false;
							}				
							
						}
					}
				}

			}
			
			if(!skipWaitV2IngestCompletion) {
				logger.info("Sleep " + sleepTime + " [s]");
				Thread.sleep(sleepTime*1000);
			}
		} while (!skipWaitV2IngestCompletion);
		
		logger.traceExit();

	}
	
	public static void getV2IngestResult(String sessionId, String baseUrl, String apiVersion, String jobName, String tmpDir, String outputDir) throws FileNotFoundException, IOException {
		logger.traceEntry();
		String fileName = tmpDir + jobName + ".json";
		File jsonFile = new File(fileName);
		
		boolean skipGetV2IngestResult = !jsonFile.exists();
		
		if (skipGetV2IngestResult) {
			logger.error(fileName + "does not exist. Job must be created");
		}
		
		if(skipGetV2IngestResult) {
			logger.warn("skip");
			logger.traceExit();
			return;
		}
		
		skipGetV2IngestResult = true;
		
		JsonObject jsonObject = null;
		JsonObject innerObject = null;
		try (FileReader fr = new FileReader(jsonFile)) {
			
			JsonElement jsonElement = JsonParser.parseReader(fr);
			jsonObject = jsonElement.getAsJsonObject();
		}
		innerObject = jsonObject.getAsJsonObject("ingest");
			
		for(Entry<String, JsonElement> curEntry : innerObject.entrySet()) {
			
			JsonElement ingestElement = curEntry.getValue();
			if(ingestElement.isJsonObject()) {
				JsonObject ingestObject = ingestElement.getAsJsonObject();
				if(ingestObject.has("id")) {
					
					if (ingestObject.has("state")) {
						String jobId = ingestObject.get("id").getAsString();
						switch (ingestObject.get("state").getAsString()) {
						case "Aborted":
						case "Failed":
						{
							String jobResponse = unprocessedrecords(sessionId, baseUrl, apiVersion, jobId);
							
							ZipOutputStream outZip =  new ZipOutputStream(new FileOutputStream(outputDir + jobName + "_" + jobId + "_unprocessed.zip"));
							outZip.putNextEntry(new ZipEntry(jobName + "_" + jobId + "_unprocessed.csv"));
							BufferedWriter buffWriter = new BufferedWriter(new OutputStreamWriter(outZip, StandardCharsets.ISO_8859_1));
							buffWriter.write(jobResponse);
							buffWriter.flush();
							buffWriter.close();
							outZip.flush();
							outZip.close();
						}
						case "JobComplete":
							long numberRecordsFailed = Long.parseLong(ingestObject.get("numberRecordsFailed").getAsString());
							if(numberRecordsFailed > 0) {
								String jobResponse = failedResults(sessionId, baseUrl, apiVersion, jobId);
								
								ZipOutputStream outZip =  new ZipOutputStream(new FileOutputStream(outputDir + jobName + "_" + jobId + "_failed.zip"));
								outZip.putNextEntry(new ZipEntry(jobName + "_" + jobId + "_failed.csv"));
								BufferedWriter buffWriter = new BufferedWriter(new OutputStreamWriter(outZip, StandardCharsets.ISO_8859_1));
								buffWriter.write(jobResponse);
								buffWriter.flush();
								buffWriter.close();
								outZip.flush();
								outZip.close();
								
							}
							
							long numberRecordsProcessed = Long.parseLong(ingestObject.get("numberRecordsProcessed").getAsString());
							if((numberRecordsProcessed - numberRecordsFailed) > 0) {
								String jobResponse = successfulResults(sessionId, baseUrl, apiVersion, jobId);
								ZipOutputStream outZip =  new ZipOutputStream(new FileOutputStream(outputDir + jobName + "_" + jobId + "_successful.zip"));
								outZip.putNextEntry(new ZipEntry(jobName + "_" + jobId + "_successful.csv"));
								BufferedWriter buffWriter = new BufferedWriter(new OutputStreamWriter(outZip, StandardCharsets.ISO_8859_1));
								buffWriter.write(jobResponse);
								buffWriter.flush();
								buffWriter.close();
								outZip.flush();
								outZip.close();
							}
						}				
						
					}
				}
			}

		}				
		

		
		logger.traceExit();
		
	}
	
}
