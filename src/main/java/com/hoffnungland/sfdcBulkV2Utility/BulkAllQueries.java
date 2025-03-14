package com.hoffnungland.sfdcBulkV2Utility;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.io.FileWriter;
import java.io.Writer;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class BulkAllQueries {
	
	private static final Logger logger = LogManager.getLogger(BulkAllQueries.class);
	
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
		String outputDir = projectPropsFile.getProperty("outputDir");
		
		
		
		try {
			String nextRecordsUrl = null;
			boolean done = false;
			String fileBulkAllQueries = outputDir + "bulkAllQueries.csv";
			
			// Define the CSV header. Ensure you include all potential fields.
	        String[] headers = {"id", "operation", "object", "createdById", "createdDate", 
	                            "systemModstamp", "state", "concurrencyMode", "contentType", 
	                            "apiVersion", "jobType"};
	        
			CSVFormat csvFormatQuery = CSVFormat.Builder.create().setHeader(headers).setQuoteMode(QuoteMode.ALL).setDelimiter(";").get();
			try (Writer writer = new FileWriter(fileBulkAllQueries);
	             CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormatQuery)) {
				
				do {
					
					String response = V2Query.getJobsInfo(sessionId, baseUrl, apiVersion, nextRecordsUrl);
					JsonElement jsonRootElement = JsonParser.parseString(response);
					JsonObject jsonRootObject = jsonRootElement.getAsJsonObject();
					JsonElement nextRecordJsonEl = jsonRootObject.get("nextRecordsUrl"); 
					nextRecordsUrl = nextRecordJsonEl.isJsonNull() ? null : nextRecordJsonEl.getAsString();
					done = jsonRootObject.get("done").getAsBoolean();
					JsonArray records = jsonRootObject.get("records").getAsJsonArray();
					parseResponse(records, csvPrinter);
		            
		        } while(!done);
		    }
			logger.info("CSV file created successfully.");
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			String nextRecordsUrl = null;
			boolean done = false;
			String fileBulkAllIngest = outputDir + "bulkAllIngests.csv";
			
			// Define the CSV header. Ensure you include all potential fields.
	        String[] headers = {"id", "operation", "object", "createdById", "createdDate", 
	                            "systemModstamp", "state", "concurrencyMode", "contentType", 
	                            "apiVersion", "jobType"};
	        
			CSVFormat csvFormatQuery = CSVFormat.Builder.create().setHeader(headers).setQuoteMode(QuoteMode.ALL).setDelimiter(";").get();
			try (Writer writer = new FileWriter(fileBulkAllIngest);
	             CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormatQuery)) {
				
				do {
					
					String response = V2Ingest.getJobsInfo(sessionId, baseUrl, apiVersion, nextRecordsUrl);
					JsonElement jsonRootElement = JsonParser.parseString(response);
					JsonObject jsonRootObject = jsonRootElement.getAsJsonObject();
					JsonElement nextRecordJsonEl = jsonRootObject.get("nextRecordsUrl"); 
					nextRecordsUrl = nextRecordJsonEl.isJsonNull() ? null : nextRecordJsonEl.getAsString();
					done = jsonRootObject.get("done").getAsBoolean();
					JsonArray records = jsonRootObject.get("records").getAsJsonArray();
					parseResponse(records, csvPrinter);
		            
		        } while(!done);
		    }
			logger.info("CSV file created successfully.");
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		logger.traceExit();
		
	}
	
	public static void parseResponse(JsonArray records, CSVPrinter csvPrinter) throws IOException {
		
		logger.traceEntry();
		
        // Write to CSV using a try-with-resources block


		// Loop through each record
        for(JsonElement curRecord : records) {
        	JsonObject recordObj = curRecord.getAsJsonObject();
            
            // Retrieve values from the JSON object; if a key doesn't exist, default to an empty string.
            String id = recordObj.get("id").getAsString();
            String operation = recordObj.get("operation").getAsString();
            String objectType = recordObj.get("object").getAsString();
            String createdById = recordObj.get("createdById").getAsString();
            String createdDate = recordObj.get("createdDate").getAsString();
            String systemModstamp = recordObj.get("systemModstamp").getAsString();
            String state = recordObj.get("state").getAsString();
            String concurrencyMode = recordObj.get("concurrencyMode").getAsString();
            String contentType = recordObj.get("contentType").getAsString();
            String jsonApiVersion = recordObj.get("apiVersion").getAsString();
            String jobType = recordObj.get("jobType").getAsString();

            // Print the CSV record
            csvPrinter.printRecord(id, operation, objectType, createdById, createdDate, 
                    systemModstamp, state, concurrencyMode, contentType, jsonApiVersion, 
                    jobType);
        }
        
        // Flush the printer to ensure all data is written
        csvPrinter.flush();
		
		logger.traceExit();
		
	}
}
