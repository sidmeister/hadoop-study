package com.hotwire.biti.gcs;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.io.Text;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.jdbc.JDBCScheme;
import cascading.jdbc.JDBCTap;
import cascading.jdbc.TableDesc;
import cascading.operation.aggregator.Count;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.LeftJoin;
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
		String bucket = args[0];
		String processing_hour = args[1];
		AppProperties app = new AppProperties();
		Properties prop = app.readProperties();
		String aws_access_key_id = prop.getProperty("accessKeyId");
		String aws_secret_access_key = prop.getProperty("secretKey");
		String dw_host = prop.getProperty("dw_host");
		String dw_service = prop.getProperty("dw_service");
		String url = "jdbc:oracle:thin:@//" + dw_host + ":" + "1521/" + dw_service;
		String dw_user= prop.getProperty("dw_user");
		String dw_password= prop.getProperty("dw_password");
		String driver = "oracle.jdbc.driver.OracleDriver";
	    String outputPath =  "s3://" + bucket + "/data/input/rawdata/gcs/bexredirect/" + processing_hour + "/";
		String inputPath =  "s3://" + bucket + "/data/intermediate/rawdata/gcs/bexredirect/";
		prop.setProperty("fs.s3.awsAccessKeyId", aws_access_key_id);
		prop.setProperty("fs.s3.awsSecretAccessKey", aws_secret_access_key);
		AppProps.setApplicationJarClass(prop, BexRedirectGCS.class);
		Tap bexsrcTap = new Hfs(new WritableSequenceFile(new Fields("line"),Text.class), inputPath);
		String tableName = "int_gcs_search";
		 //Columns I want to write to 
		String[] columnDefs = {"VARCHAR(100)","VARCHAR(100)","VARCHAR(100)","VARCHAR(100)","VARCHAR(100)","VARCHAR(100)","VARCHAR(100)","VARCHAR(100)","VARCHAR(100)","VARCHAR(100)","VARCHAR(100)","VARCHAR(4000)","VARCHAR(4000)","VARCHAR(100)","VARCHAR(4000)","VARCHAR(500)","VARCHAR2(10)","VARCHAR2(10)","VARCHAR2(10)","VARCHAR2(30)","VARCHAR2(50)","VARCHAR2(50)","VARCHAR2(50)","VARCHAR2(50)","VARCHAR2(10)","VARCHAR2(15)"};
		String[] columnNames = {"SEARCH_ID","SEARCH_DATE","CLASS","DEST_CITY_ID","ORIG_CITY_ID","START_DATE","END_DATE","CUSTOMER_ID","DESTINATION_ID","CLIENT_ID","SITE_TYPE","RAW_USER_AGENT_STRING","ENCRYPTED_DISCOUNT_HASH","DEVICE_TYPE","URL","REDIRECT_TYPE","CUST_GEOINFOSOURCE","CUST_COUNTRYCODE","CUST_GMTOFFSET","CUST_ZIPCODE","CUST_CITYNAME","CUST_STATENAME","CUST_LATITUDE","CUST_LONGITUDE","CUST_NDMACODE","CUST_IPADDRESS"};
		TableDesc tableDesc = new TableDesc( tableName,columnNames,columnDefs,null);
		JDBCScheme dbScheme = new JDBCScheme( columnNames );
		Tap dbSink = new JDBCTap( url, dw_user,dw_password,driver, tableDesc, dbScheme);
		Tap s3Sink = new Hfs(new TextDelimited(false, "@~@"), outputPath,SinkMode.REPLACE);
		Hadoop2MR1FlowConnector flowconnector = new Hadoop2MR1FlowConnector(prop);
		FlowDef flowdef = createWorkflow(bexsrcTap, dbSink, s3Sink);
		Flow flow = flowconnector.connect(flowdef);
		flow.complete();
      
	} 
	
	
public static FlowDef createWorkflow(Tap bexsrcTap, Tap s3Sink, Tap dbSink ){
		
	Fields SourceFieldsIdx = new Fields("SEARCH_ID","SEARCH_DATE","CLASS","DEST_CITY_ID","ORIG_CITY_ID","START_DATE","END_DATE","CUSTOMER_ID","DESTINATION_ID","CLIENT_ID","SITE_TYPE","RAW_USER_AGENT_STRING","ENCRYPTED_DISCOUNT_HASH","DEVICE_TYPE","URL","REDIRECT_TYPE","CUST_GEOINFOSOURCE","CUST_COUNTRYCODE","CUST_GMTOFFSET","CUST_ZIPCODE","CUST_CITYNAME","CUST_STATENAME","CUST_LATITUDE","CUST_LONGITUDE","CUST_NDMACODE","CUST_IPADDRESS");
	ScrubFunction scrubFunc = new ScrubFunction(SourceFieldsIdx);
  	RegexSplitter regexSplitter = new RegexSplitter(SourceFieldsIdx,"@~@");
  	Pipe searchPipe = new Each( "BexRedirectGCS", new Fields("line"),regexSplitter);
  	searchPipe = new Each( searchPipe,SourceFieldsIdx,scrubFunc,Fields.REPLACE );
  	Pipe s3pipe = new Pipe("s3pipe",searchPipe );
  	Pipe dbpipe = new Pipe("dbpipe",searchPipe );
  	FlowDef flowdef = FlowDef.flowDef();
  	flowdef.addSource(searchPipe, bexsrcTap)
    	   .addTailSink(s3pipe, s3Sink)
           .addTailSink(dbpipe, dbSink);
	  		
	    return flowdef;
	}
}
