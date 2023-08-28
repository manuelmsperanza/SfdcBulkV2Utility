package com.hoffnungland.sfdcBulkV2Utility;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App 
{
		public static void main(String[] args) {
		
		String sessionId = "";
		String baseUrl = "";
		String jobId = "";
		String apiVersion = "v58.0";
		String[] queryLocators = {null};
		String csvContent = "";
		String archiveFilePath = "./";
		String archiveFilenamePrefix = ".";
		String delTmpFilenamePrefix = "";
		try{
			
			//String response = V2Ingest.checkLimits(sessionId, baseUrl, apiVersion);
			//String response = V2Ingest.createJob(sessionId, baseUrl, apiVersion, "Offerte_Promozioni__c", "CSV", "update", "SEMICOLON");
			//String response = V2Ingest.uploadCsvContent(sessionId, baseUrl, apiVersion, jobId, csvContent);
			//String response = V2Ingest.changeJobStatus(sessionId, baseUrl, apiVersion, jobId, "UploadComplete");
			
			CsvArchiver csvArchiver = new CsvArchiver();
			csvArchiver.initialize(archiveFilePath, archiveFilenamePrefix, delTmpFilenamePrefix);
			
			
			//java.io.InputStream targetStream = new java.io.ByteArrayInputStream(response.getBytes(java.nio.charset.StandardCharsets.UTF_8));
			//java.io.Reader targetStreamReader = new InputStreamReader(new java.io.ByteArrayInputStream(response.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1)), java.nio.charset.StandardCharsets.ISO_8859_1);
			//java.io.Reader stringReader = new StringReader(response);
			
			int loopIdx = 0;
			
			for(String curQueryLocator : queryLocators) {
				
				String response = V2Ingest.getQueryResult(sessionId, baseUrl, apiVersion, jobId, curQueryLocator, -1);
				//System.out.println(response);
				
				csvArchiver.parseResponse(response, loopIdx);
				loopIdx++;
			}
			
			csvArchiver.finalize();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
