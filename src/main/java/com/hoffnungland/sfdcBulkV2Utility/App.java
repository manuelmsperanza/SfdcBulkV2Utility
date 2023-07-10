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
		String csvContent = "";
		try{
			
			String response = V2Ingest.checkLimits(sessionId, baseUrl, apiVersion);
			//String response = V2Ingest.createJob(sessionId, baseUrl, apiVersion, "Offerte_Promozioni__c", "CSV", "update", "SEMICOLON");
			//String response = V2Ingest.uploadCsvContent(sessionId, baseUrl, apiVersion, jobId, csvContent);
			//String response = V2Ingest.changeJobStatus(sessionId, baseUrl, apiVersion, jobId, "UploadComplete");
			
			System.out.println(response);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
