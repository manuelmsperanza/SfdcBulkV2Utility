package com.hoffnungland.sfdcBulkV2Utility;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CsvArchiver implements AutoCloseable {
	
	private static final Logger logger = LogManager.getLogger(CsvArchiver.class);
	
	CSVFormat csvFormatRetrieve;
	CSVFormat csvFormatArchive;
	CSVFormat csvFormatOnlyId;
	ZipOutputStream outBulkZip;
	BufferedWriter buffWriter;
	CSVPrinter csvPrinterArchive;
	CSVPrinter csvPrinterOnlyId;
	int idFieldPosition = -1;
	
	public void initialize(String archiveFilePath, String archiveFilenamePrefix, String onlyIdFilenamePrefix, char retrieveDelimiterChar, char archiveDelimiterChar) throws IOException {
		
		logger.traceEntry();
		
		this.csvFormatRetrieve = CSVFormat.Builder.create().setQuoteMode(QuoteMode.ALL).setDelimiter(retrieveDelimiterChar).get();
		this.csvFormatArchive = CSVFormat.Builder.create().setQuoteMode(QuoteMode.ALL).setDelimiter(archiveDelimiterChar).get();
		
		String pattern = "yyyyMMddHHmmss";
		DateFormat dateFormat = new SimpleDateFormat(pattern);
		String archivingDate = dateFormat.format(new java.util.Date());
		
		this.outBulkZip = new ZipOutputStream(new FileOutputStream(archiveFilePath + archiveFilenamePrefix + "_" + archivingDate + ".zip"));
		this.outBulkZip.putNextEntry(new ZipEntry(archiveFilenamePrefix + ".csv"));
		this.buffWriter = new BufferedWriter(new OutputStreamWriter(this.outBulkZip, StandardCharsets.ISO_8859_1));
		this.csvPrinterArchive = this.csvFormatArchive.print(this.buffWriter);
		
		if(onlyIdFilenamePrefix != null && !onlyIdFilenamePrefix.isEmpty()) {
			this.csvFormatOnlyId = CSVFormat.Builder.create().setQuoteMode(QuoteMode.NONE).setEscape('"').setDelimiter('|').get();
			this.csvPrinterOnlyId = new CSVPrinter(new FileWriter(onlyIdFilenamePrefix + ".csv"), this.csvFormatOnlyId);
		}
		logger.traceExit();
	}
	
	public void close() throws IOException {
		logger.traceEntry();
		this.csvPrinterArchive.close();
		
		if(this.csvPrinterOnlyId != null) {
			this.csvPrinterOnlyId.close();
		}
		
		this.buffWriter.close();
		logger.traceExit();
	}
	
	public void parseResponse(String response, int loopIdx) throws IOException {
		logger.traceEntry();
		boolean isHeader = true;
		CSVParser parser = CSVParser.parse(response, this.csvFormatRetrieve);
		for(CSVRecord curRecord : parser.getRecords()) {
			logger.trace(curRecord);
			boolean doPrint = true;
			String[] recordValues = curRecord.values();
			if(isHeader) {
				isHeader = false;
				if(loopIdx == 0) {
					int tmpIdFieldPos = -1;
					for(String curRecordValue : recordValues) {
						tmpIdFieldPos++;
						if(curRecordValue.equalsIgnoreCase("id")) {
							this.idFieldPosition = tmpIdFieldPos;
						}
						recordValues[tmpIdFieldPos] = curRecordValue.replace('.', '_');
					}
				} else {
					doPrint = false;
				}
			}
			
			if(doPrint) {
				this.csvPrinterArchive.printRecord( Arrays.asList(recordValues));
				if(this.csvPrinterOnlyId != null) {
					this.csvPrinterOnlyId.printRecord(recordValues[this.idFieldPosition]);
				}
			}
			
		}
		this.csvPrinterArchive.flush();
		if(this.csvPrinterOnlyId != null) {
			this.csvPrinterOnlyId.flush();
		}
		logger.traceExit();
	}
	
}
