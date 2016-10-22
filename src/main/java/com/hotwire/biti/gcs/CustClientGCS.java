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
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.LeftJoin;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.WritableSequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class CustClientGCS {
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
			prop.setProperty("fs.s3.awsAccessKeyId", aws_access_key_id);
			prop.setProperty("fs.s3.awsSecretAccessKey", aws_secret_access_key);
			AppProps.setApplicationJarClass(prop, BexRedirectGCS.class);
			String driver = "oracle.jdbc.driver.OracleDriver";
		    String outputPath = "s3://" + bucket + "/data/input/rawdata/gcs/custclient/" + processing_hour + "/";
			String inputPath = "s3://" + bucket + "/data/intermediate/rawdata/gcs/custclient/";
			Tap bexsrcTap = new Hfs(new WritableSequenceFile(new Fields("line"),Text.class), inputPath);
			//set db parameters and db sink
			String tableName = "int_gcs_cust_client";
			 //Columns I want to write to 
			String[] columnDefs = {"VARCHAR(100)","VARCHAR(100)","VARCHAR(100)"};
			String[] columnNames = {"CLIENT_ID","CUSTOMER_ID","TIMESTAMP"};
			TableDesc tableDesc = new TableDesc( tableName,columnNames,columnDefs,null);
			JDBCScheme dbScheme = new JDBCScheme( columnNames );
			Tap dbSink = new JDBCTap( url, dw_user,dw_password,driver, tableDesc, dbScheme);
			Tap s3Sink = new Hfs(new TextDelimited(false, "@~@"), outputPath,SinkMode.REPLACE);
			Fields SourceFieldsIdx = new Fields("CLIENT_ID","CUSTOMER_ID","TIMESTAMP");
			ScrubFunction scrubFunc = new ScrubFunction(SourceFieldsIdx);
		  	RegexSplitter regexSplitter = new RegexSplitter(SourceFieldsIdx,"@~@");
		  	Pipe custClientPipe = new Each( "custClientGCS", new Fields("line"),regexSplitter);
		  	custClientPipe = new Each( custClientPipe,SourceFieldsIdx,scrubFunc,Fields.REPLACE );
		  	Pipe s3pipe = new Pipe("s3pipe",custClientPipe );
		  	Pipe dbpipe = new Pipe("dbpipe",custClientPipe );
		  	
		  	FlowDef flowdef = FlowDef.flowDef();
		  	flowdef.addSource(custClientPipe, bexsrcTap)
	        	   .addTailSink(s3pipe, s3Sink)
	               .addTailSink(dbpipe, dbSink); 
	      		//	.addTailSink(dfpipe, dbSink);

	      Hadoop2MR1FlowConnector flowconnector = new Hadoop2MR1FlowConnector(prop);
	      Flow flow = flowconnector.connect(flowdef);
	      flow.complete();
	}
}

