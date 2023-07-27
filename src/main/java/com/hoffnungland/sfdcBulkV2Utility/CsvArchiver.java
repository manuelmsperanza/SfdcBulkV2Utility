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
	CSVFormat csvFormatTmpDel;
	ZipOutputStream outBulkAccountErrorIndivZip;
	BufferedWriter buffWriter;
	CSVPrinter csvPrinterArchive;
	CSVPrinter csvPrinterTmpDel;
	int idFieldPosition = -1;
	
	public void initialize(String archiveFilePath, String archiveFilenamePrefix, String delTmpFilenamePrefix) throws IOException {
		this.csvFormatRetrieve = CSVFormat.Builder.create().setQuoteMode(QuoteMode.ALL).setDelimiter(';').build();
		this.csvFormatArchive = CSVFormat.Builder.create().setQuoteMode(QuoteMode.ALL).setDelimiter('|').build();
		this.csvFormatTmpDel = CSVFormat.Builder.create().setQuoteMode(QuoteMode.NONE).setEscape('"').setDelimiter('|').build();
		
		String pattern = "yyyyMMddHHmmss";
		DateFormat dateFormat = new SimpleDateFormat(pattern);
		String archivingDate = dateFormat.format(new java.util.Date());
		
		this.outBulkAccountErrorIndivZip = new ZipOutputStream(new FileOutputStream(archiveFilePath + archiveFilenamePrefix + "_" + archivingDate + ".zip"));
		this.outBulkAccountErrorIndivZip.putNextEntry(new ZipEntry(archiveFilenamePrefix + ".csv"));
		this.buffWriter = new BufferedWriter(new OutputStreamWriter(this.outBulkAccountErrorIndivZip, StandardCharsets.ISO_8859_1));
		this.csvPrinterArchive = this.csvFormatArchive.print(this.buffWriter);
		this.csvPrinterTmpDel = new CSVPrinter(new FileWriter(delTmpFilenamePrefix + ".csv"), this.csvFormatTmpDel);
		
	}
	
	public void finalize() throws IOException {
		csvPrinterArchive.close();
		
		csvPrinterTmpDel.close();
		buffWriter.close();
	}
	
	public void parseResponse(String response, int loopIdx) throws IOException {
		boolean isHeader = true;
		CSVParser parser = CSVParser.parse(response, csvFormatRetrieve);
		for(CSVRecord curRecord : parser.getRecords()) {
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
				csvPrinterArchive.printRecord(recordValues);
				csvPrinterTmpDel.printRecord(recordValues[this.idFieldPosition]);
			}
			
		}
		csvPrinterArchive.flush();
		csvPrinterTmpDel.flush();
	}
	
	
}
