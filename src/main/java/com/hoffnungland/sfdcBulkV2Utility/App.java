package com.hoffnungland.sfdcBulkV2Utility;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Hello world!
 *
 */
public class App 
{
		
	private static final Logger logger = LogManager.getLogger(App.class);
	
	public static void main(String[] args) {
		
		logger.traceEntry();
			
		String sessionId = "";
		String baseUrl = "";
		String apiVersion = "v61.0";
		String jobName = "";
		int sleepTime = 60;
		String operation = "query";
		String query = "";
		String columnDelimiter = "PIPE";
		String tmpDir = "C:/TOS/Data/tmp/";
		String outputDir = "C:/TOS/Data/output/";
		String archiveFilenamePrefix = "";
		String delTmpFilenamePrefix = null;
		
		try{
			
			V2Query.extractV2Query(sessionId, baseUrl, apiVersion, jobName, sleepTime, operation, query, columnDelimiter, tmpDir, outputDir, archiveFilenamePrefix, delTmpFilenamePrefix);
			
			String tmpFileName = tmpDir + jobName + ".json";
			File jsonFile = new File(tmpFileName);
		    if (jsonFile.delete()) { 
		      logger.info("Deleted the file: " + jsonFile.getName());
		    } else {
		      logger.error("Failed to delete the file.");
		    } 
			
		} catch (IOException | InterruptedException e) {
			logger.error(e);
		} 

		logger.traceExit();
	}
}
