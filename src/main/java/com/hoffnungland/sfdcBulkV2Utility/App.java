package com.hoffnungland.sfdcBulkV2Utility;

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
		String apiVersion = "v59.0";
		String jobId = "";
		String jobName = "";
		int sleepTime = 60;
		String query = "";
		String columnDelimiter = "PIPE";
		String tmpDir = "";
		String outputDir = "";
		String archiveFilenamePrefix = ".";
		String delTmpFilenamePrefix = "";
		
		try{
			
			V2Query.extractV2Query(sessionId, baseUrl, apiVersion, jobName, sleepTime, query, columnDelimiter, tmpDir, outputDir, archiveFilenamePrefix, delTmpFilenamePrefix);
			
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		logger.traceExit();
	}
}
