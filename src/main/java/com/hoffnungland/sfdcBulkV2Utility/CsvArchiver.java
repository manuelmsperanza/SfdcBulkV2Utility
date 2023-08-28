package com.hoffnungland.sfdcBulkV2Utility;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;

public class CsvArchiver {
	
	CSVFormat csvFormatRetrieve;
	CSVFormat csvFormatArchive;
	CSVFormat csvFormatOnlyId;
	ZipOutputStream outBulkZip;
	BufferedWriter buffWriter;
	CSVPrinter csvPrinterArchive;
	CSVPrinter csvPrinterOnlyId;
	int idFieldPosition = -1;
	
	public void initialize(String archiveFilePath, String archiveFilenamePrefix, String onlyIdFilenamePrefix) throws IOException {
		this.csvFormatRetrieve = CSVFormat.Builder.create().setQuoteMode(QuoteMode.ALL).setDelimiter(';').build();
		this.csvFormatArchive = CSVFormat.Builder.create().setQuoteMode(QuoteMode.ALL).setDelimiter('|').build();
		
		
		String pattern = "yyyyMMddHHmmss";
		DateFormat dateFormat = new SimpleDateFormat(pattern);
		String archivingDate = dateFormat.format(new java.util.Date());
		
		this.outBulkZip = new ZipOutputStream(new FileOutputStream(archiveFilePath + archiveFilenamePrefix + "_" + archivingDate + ".zip"));
		this.outBulkZip.putNextEntry(new ZipEntry(archiveFilenamePrefix + ".csv"));
		this.buffWriter = new BufferedWriter(new OutputStreamWriter(this.outBulkZip, StandardCharsets.ISO_8859_1));
		this.csvPrinterArchive = this.csvFormatArchive.print(this.buffWriter);
		
		if(onlyIdFilenamePrefix != null) {
			this.csvFormatOnlyId = CSVFormat.Builder.create().setQuoteMode(QuoteMode.NONE).setEscape('"').setDelimiter('|').build();
			this.csvPrinterOnlyId = new CSVPrinter(new FileWriter(onlyIdFilenamePrefix + ".csv"), this.csvFormatOnlyId);
		}
	}
	
	public void finalize() throws IOException {
		this.csvPrinterArchive.close();
		
		if(this.csvPrinterOnlyId != null) {
			this.csvPrinterOnlyId.close();
		}
		
		this.buffWriter.close();
	}
	
	public void parseResponse(String response, int loopIdx) throws IOException {
		boolean isHeader = true;
		CSVParser parser = CSVParser.parse(response, csvFormatRetrieve);
		for(CSVRecord curRecord : parser.getRecords()) {
			//System.out.println(curRecord);
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
				this.csvPrinterArchive.printRecord(recordValues);
				if(this.csvPrinterOnlyId != null) {
					this.csvPrinterOnlyId.printRecord(recordValues[this.idFieldPosition]);
				}
			}
			
		}
		this.csvPrinterArchive.flush();
		if(this.csvPrinterOnlyId != null) {
			this.csvPrinterOnlyId.flush();
		}
	}
	
	
}
