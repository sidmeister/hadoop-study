package com.sid.hadoop;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.io.Text;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.WritableSequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tap.hadoop.Hfs;;

public class BexRedirectGCS {
	public static void main(String args[]) throws IOException
	{
		//set s3 parameters and s3 sink
		AppProperties app = new AppProperties();
		Properties prop = app.readProperties();
		String outputPath =  "/Users/srathi/hadoop/out";
		//String inputPath =  "/user/srathi/attribution-redirect-bex-output-201608181120-10.139.24.238.1471544429951.seq";
		String inputPath =  "/Users/srathi/Documents/study/hadoop/attribution-redirect-bex-output-201608181120-10.139.24.238.1471544429951.seq";
		AppProps.setApplicationJarClass(prop, BexRedirectGCS.class);
		Tap bexsrcTap = new Hfs(new WritableSequenceFile(new Fields("line"),Text.class), inputPath);
		Tap s3Sink = new Hfs(new TextDelimited(false, "@~@"), outputPath,SinkMode.REPLACE);
		Hadoop2MR1FlowConnector flowconnector = new Hadoop2MR1FlowConnector(prop);
		FlowDef flowdef = createWorkflow(bexsrcTap, s3Sink);
		Flow flow = flowconnector.connect(flowdef);
		flow.complete();
      
	} 
	
	
public static FlowDef createWorkflow(Tap bexsrcTap, Tap s3Sink){
		
	Fields SourceFieldsIdx = new Fields("SEARCH_ID","SEARCH_DATE","CLASS","DEST_CITY_ID","ORIG_CITY_ID","START_DATE","END_DATE","CUSTOMER_ID","DESTINATION_ID","CLIENT_ID","SITE_TYPE","RAW_USER_AGENT_STRING","ENCRYPTED_DISCOUNT_HASH","DEVICE_TYPE","URL","REDIRECT_TYPE","CUST_GEOINFOSOURCE","CUST_COUNTRYCODE","CUST_GMTOFFSET","CUST_ZIPCODE","CUST_CITYNAME","CUST_STATENAME","CUST_LATITUDE","CUST_LONGITUDE","CUST_NDMACODE","CUST_IPADDRESS");
	ScrubFunction scrubFunc = new ScrubFunction(SourceFieldsIdx);
  	RegexSplitter regexSplitter = new RegexSplitter(SourceFieldsIdx,"@~@");
  	Pipe searchPipe = new Each( "BexRedirectGCS", new Fields("line"),regexSplitter);
  	searchPipe = new Each( searchPipe,SourceFieldsIdx,scrubFunc,Fields.REPLACE );
  	Pipe s3pipe = new Pipe("s3pipe",searchPipe );
  	FlowDef flowdef = FlowDef.flowDef();
  	flowdef.addSource(searchPipe, bexsrcTap)
    	   .addTailSink(s3pipe, s3Sink);
	    return flowdef;
	}
}
