package com.hoffnungland.sfdcBulkV2Utility;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

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

public class V2Ingest {
	
	private static final Logger logger = LogManager.getLogger(V2Ingest.class);
	
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
	
	
	public static void bulkV2Ingest(String sessionId, String baseUrl, String apiVersion, String objectName, String contentType, String operation, String columnDelimiter, String inputFilePath) throws IOException {
		
		logger.traceEntry();
		
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
		
		StringBuilder ingestPayload = null;
		CSVPrinter csvPrinter = null;
		
		String header = null;
		long headerSize = 0;
		long stringSize = 0;
		for(CSVRecord curRecord : csvParser.getRecords()) {
			
			logger.debug(curRecord);
			
			if(ingestPayload == null) {				
				ingestPayload = new StringBuilder(header);
				stringSize = headerSize;
			}
			if(csvPrinter == null) {				
				csvPrinter = csvFormat.print(ingestPayload);
			}
			
			if(header == null) {
				header = ingestPayload.toString();
				if(header.length() > 32000) {
					logger.error("Header too long");
					logger.traceExit();
					return;
				}
				headerSize = (new Double(Math.ceil(header.getBytes().length/3)*4)).longValue();
				//stringSize = headerSize;
				logger.trace(headerSize + " " + header);
			}
			
			csvPrinter.printRecord(curRecord.values());
			
			
			/*totalRecord--;
			numberofRows++;
			String line = row2.line + "\n";
			long encodedSize = (new Double(Math.ceil(line.getBytes().length/3)*4)).longValue();
			if(stringSize + encodedSize > 150*1024*1024){
				logger.info("flush size " + stringSize);
				csvContent = strBld.toString();
				row11.outContent = Long.toString(numberofRows);
				numberofRows = 0;
				strBld = new StringBuilder();
				strBld.append(header);
				stringSize = headerSize;
				
			} else {
				row11.outContent = null;
				//row11 = null;
			}
			strBld.append(line);
			stringSize += encodedSize;
			logger.debug(stringSize + " " + encodedLine);
			if(totalRecord == 0){
				csvContent = strBld.toString();
				row11.outContent = Long.toString(numberofRows);
				numberofRows = 0;
				logger.info("flush total record " + stringSize);
			}*/
			
		}
		
		logger.traceExit();
		
	}
	
}
