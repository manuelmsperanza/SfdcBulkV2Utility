package com.hoffnungland.sfdcBulkV2Utility;

import java.io.File;
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
		String jobName = "";
		int sleepTime = 60;
		String operation = "";
		String columnDelimiter = "PIPE";
		String contentType = "CSV";
		String objectName = "";
		String tmpDir = "";
		String outputDir = "";
		String delTmpFilenamePrefix = "";
		String inputFilePath = tmpDir + delTmpFilenamePrefix + ".csv";
		
		try{
			V2Ingest.bulkV2Ingest(sessionId, baseUrl, apiVersion, jobName, sleepTime, objectName, contentType, operation, columnDelimiter, inputFilePath, tmpDir, outputDir);
			
			String tmpFileName = tmpDir + jobName + ".json";
			File jsonFile = new File(tmpFileName);
		    if (jsonFile.delete()) { 
		      logger.info("Deleted the file: " + jsonFile.getName());
		    } else {
		      logger.error("Failed to delete the file.");
		    }
		    
			File inputFileIngest = new File(inputFilePath);
		    if (inputFileIngest.delete()) { 
		      logger.info("Deleted the file: " + inputFileIngest.getName());
		    } else {
		      logger.error("Failed to delete the file.");
		    }
			
		} catch (IOException | InterruptedException e) {
			logger.error(e);
		} 

		logger.traceExit();

	}

}
