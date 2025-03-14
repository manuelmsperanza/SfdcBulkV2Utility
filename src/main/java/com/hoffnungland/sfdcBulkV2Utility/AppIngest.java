package com.hoffnungland.sfdcBulkV2Utility;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AppIngest {
	
	private static final Logger logger = LogManager.getLogger(AppIngest.class);
	
	public static void main(String[] args) {
		logger.traceEntry();
		
		if(args.length < 1){
			logger.error("Wrong input parameter. Params is: ProjectName");
			return;
		}
		
		String projectName = args[0];
		
		Properties projectPropsFile = new Properties();
		try (FileInputStream projectFile = new FileInputStream("./etc/" + projectName + ".properties")){
			projectPropsFile.load(projectFile);
			projectFile.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
		String sessionId = projectPropsFile.getProperty("sessionId");
		String baseUrl = projectPropsFile.getProperty("baseUrl");
		String apiVersion = projectPropsFile.getProperty("apiVersion");
		String jobName = projectPropsFile.getProperty("jobName");
		int sleepTime = Integer.parseInt(projectPropsFile.getProperty("sleepTime"));
		String operation = projectPropsFile.getProperty("operation");
		//String query = projectPropsFile.getProperty("query");
		String columnDelimiter = projectPropsFile.getProperty("columnDelimiter");
		String contentType = projectPropsFile.getProperty("contentType"); //"CSV";
		String objectName = projectPropsFile.getProperty("objectName");
		String tmpDir = projectPropsFile.getProperty("tmpDir");
		String outputDir = projectPropsFile.getProperty("outputDir");
		//String archiveFilenamePrefix = projectPropsFile.getProperty("archiveFilenamePrefix");
		String delTmpFilenamePrefix = projectPropsFile.getProperty("delTmpFilenamePrefix");
		
		String inputFilePath = outputDir + delTmpFilenamePrefix + ".csv";
		
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
