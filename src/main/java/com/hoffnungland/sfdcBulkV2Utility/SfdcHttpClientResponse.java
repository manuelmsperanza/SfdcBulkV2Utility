package com.hoffnungland.sfdcBulkV2Utility;


import java.util.Map;

import org.apache.hc.core5.http.Header;

public class SfdcHttpClientResponse {
	public Header[] headers;
	public Map<String, String> mapHeaders;
	public String body;
}
