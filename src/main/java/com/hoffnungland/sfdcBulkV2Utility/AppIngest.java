package com.hoffnungland.sfdcBulkV2Utility;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AppIngest {
	
	private static final Logger logger = LogManager.getLogger(AppIngest.class);
	
	public static void main(String[] args) {
		logger.traceEntry();
		
		String sessionId = "";
		String baseUrl = "";
		String apiVersion = "v59.0";
		String jobId = "";
		String jobName = "";
		int sleepTime = 60;
		String operation = "";
		String columnDelimiter = "PIPE";
		String contentType = "CSV";
		String objectName = "";
		String tmpDir = "";
		String outputDir = "";
		String archiveFilenamePrefix = "";
		String delTmpFilenamePrefix = null;
		String inputFilePath = "";
		
		try{
			V2Ingest.bulkV2Ingest(sessionId, baseUrl, apiVersion, jobName, sleepTime, objectName, contentType, operation, columnDelimiter, inputFilePath, tmpDir, outputDir);    
		    V2Ingest.waitV2IngestCompletion(sessionId, baseUrl, apiVersion, jobName, tmpDir, sleepTime);
		    V2Ingest.getV2IngestResult(sessionId, baseUrl, apiVersion, jobName, tmpDir, outputDir);
			
		} catch (IOException | InterruptedException e) {
			logger.error(e);
		} 

		logger.traceExit();

	}

}
