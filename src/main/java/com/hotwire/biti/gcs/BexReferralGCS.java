package com.hotwire.biti.gcs;


import java.io.IOException;
import java.util.Properties;

import cascading.operation.expression.ExpressionFilter;
import org.apache.hadoop.io.Text;

import com.hotwire.biti.gcs.ScrubFunction;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.jdbc.JDBCScheme;
import cascading.jdbc.JDBCTap;
import cascading.jdbc.TableDesc;
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

public class BexReferralGCS {
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
	    String outputPath =  "s3://" + bucket + "/data/input/rawdata/gcs/bexreferral/" + processing_hour + "/";
		String inputPath =  "s3://" + bucket + "/data/intermediate/rawdata/gcs/bexreferral/";
		prop.setProperty("fs.s3.awsAccessKeyId", aws_access_key_id);
		prop.setProperty("fs.s3.awsSecretAccessKey", aws_secret_access_key);
		AppProps.setApplicationJarClass(prop, BexReferralGCS.class);	
		Tap bexsrcTap = new Hfs(new WritableSequenceFile(new Fields("line"),Text.class), inputPath);
		//set db parameters and db sink
		String tableName = "int_gcs_search_referral";
		 //Columns I want to write to 
		String[] columnDefs = {"VARCHAR(100)","VARCHAR(100)","VARCHAR(100)","VARCHAR(100)","VARCHAR(100)","VARCHAR(100)","VARCHAR(100)","VARCHAR(100)","VARCHAR(100)","VARCHAR(100)","VARCHAR(100)","VARCHAR(100)","VARCHAR(100)","VARCHAR(4000)","VARCHAR(1000)"};
		String[] columnNames = {"SEARCH_ID","REFERRAL_TYPE","REFERRER_ID","LINK_ID","VERSION_ID","KEYWORD_ID","MATCH_TYPE_ID","REFERRAL_DATE","REFERRAL_SITE_ID","AFFILIATE_NETWORK_ID","KEYWORD_ACCOUNT_ID","KEYWORD_CAMPAIGN_ID","REFERRING_HOTEL_ID","SEARCH_ENGINE","SEO_FLAG"};
		TableDesc tableDesc = new TableDesc( tableName,columnNames,columnDefs,null);
		JDBCScheme dbScheme = new JDBCScheme( columnNames );
		Tap dbSink = new JDBCTap( url, dw_user,dw_password,driver, tableDesc, dbScheme);
		Tap s3Sink = new Hfs(new TextDelimited(false, "@~@"), outputPath,SinkMode.REPLACE);
		Hadoop2MR1FlowConnector flowconnector = new Hadoop2MR1FlowConnector(prop);
		FlowDef flowDef = createWorkflow(bexsrcTap, dbSink, s3Sink);
		flowconnector.connect( flowDef ).complete();  
	  
	} 
	
	public static FlowDef createWorkflow(Tap bexsrcTap, Tap s3Sink, Tap dbSink ){
		
		Fields SourceFieldsIdx = new Fields("SEARCH_ID","REFERRAL_TYPE","REFERRER_ID","LINK_ID","VERSION_ID","KEYWORD_ID","MATCH_TYPE_ID","REFERRAL_DATE","REFERRAL_SITE_ID","AFFILIATE_NETWORK_ID","KEYWORD_ACCOUNT_ID","KEYWORD_CAMPAIGN_ID","REFERRING_HOTEL_ID","SEARCH_ENGINE","SEO_FLAG");
		ScrubFunction scrubFunc = new ScrubFunction(SourceFieldsIdx);
	  	RegexSplitter regexSplitter = new RegexSplitter(SourceFieldsIdx,"@~@");
	  	Pipe dfpipe = new Each( "BexReferralGCS", new Fields("line"),regexSplitter);
	  	dfpipe = new Each( dfpipe,SourceFieldsIdx,scrubFunc,Fields.REPLACE );
		ExpressionFilter valid_seo = new ExpressionFilter("SEARCH_ENGINE.length() > 4000",
				new String[] { "SEARCH_ENGINE"},
				new Class[] { String.class}
				);
		dfpipe = new Each(dfpipe, valid_seo);
	  	Pipe s3pipe = new Pipe("s3pipe",dfpipe );
	  	Pipe dbpipe = new Pipe("dbpipe",dfpipe );
	  	FlowDef flowdef = FlowDef.flowDef();
	  	flowdef.addSource(dfpipe, bexsrcTap)
        	   .addTailSink(s3pipe, s3Sink)
               .addTailSink(dbpipe, dbSink); 
	  		
	    return flowdef;
	}
	
}
