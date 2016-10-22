package com.hotwire.biti.gcs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import cascading.CascadingTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.property.AppProps;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeIterator;

public class BexRedirectGCSTest extends CascadingTestCase{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

    public BexRedirectGCSTest () {
        super( "distinct locations count tests" );
    }
    
    public void testData() throws IOException  {
    	String bexReferrals =  "1457635540835\t6257994064607753395@~@2016-03-10T09:25:39-08:00@~@H@~@null@~@null@~@null@~@null@~@null@~@null@~@3603291723490683036@~@null@~@Mozilla/5.0 (Windows NT 6.1; WOW64; rv:32.0) Gecko/20100101 Firefox/32.0@~@null@~@1200@~@https://www.qa.hotwire.com/Hotel-Search?sid=B378435&tmid=18028592452&paandi=true&bid=S270@~@bex@~@Q@~@US@~@-6.0@~@36107@~@montgomery@~@al@~@32.38315963745117@~@-86.28196716308594@~@698@~@68.184.77.220\n" //with geo
                                  +"1457635540834\t6257994064607753394@~@2016-03-10T09:25:39-08:00@~@H@~@null@~@null@~@null@~@null@~@null@~@null@~@3603291723490683036@~@null@~@Mozilla/5.0 (Windows NT 6.1; WOW64; rv:32.0) Gecko/20100101 Firefox/32.0@~@null@~@1200@~@https://www.qa.hotwire.com/Hotel-Search?sid=B378435&tmid=18028592452&paandi=true&bid=S270@~@bex@~@null@~@null@~@null@~@null@~@null@~@null@~@null@~@null@~@null@~@0.0.0.0\n"  //empty geo
                                 +"1457635540833\t6257994064607753393@~@2016-03-10T09:25:39-08:00@~@H@~@null@~@null@~@null@~@null@~@null@~@null@~@3603291723490683036@~@null@~@Mozilla/5.0 (Windows NT 6.1; WOW64; rv:32.0) Gecko/20100101 Firefox/32.0@~@null@~@1200@~@https://www.qa.hotwire.com/Hotel-Search?sid=B378435&tmid=18028592452&paandi=true&bid=S270@~@bex@~@Q@~@US@~@-6.0@~@36107@~@montgomery@~@al@~@null@~@null@~@698@~@68.184.77.220"; //partial geo


        //Tap users = new FileTap(new TextDelimited(bex_fields,false,"@~@"), createTempFile(bexReferral, "users_test.txt"));;
    	Tap redirects = new FileTap(new TextDelimited(new Fields("offset","line")), createTempFile(bexReferrals, "redirects.txt"));
        Tap s3out = new FileTap(new TextDelimited(false, "@~@"), "target/s3_redirect.txt",SinkMode.REPLACE);
        Tap dbout = new FileTap(new TextDelimited(false, "@~@"), "target/db_redirect.txt",SinkMode.REPLACE);

        Properties properties = new Properties();
  	    AppProps.setApplicationJarClass( properties, BexRedirectGCSTest.class );
  	    FlowConnector flowConnector = new LocalFlowConnector(properties);
          
        FlowDef flowDef = BexRedirectGCS.createWorkflow(redirects, s3out,dbout);
        Flow flow = flowConnector.connect( flowDef );
        flow.complete();
        validateLength(flow, 3);
       // {"SEARCH_ID","SEARCH_DATE","CLASS","DEST_CITY_ID","ORIG_CITY_ID","START_DATE","END_DATE","CUSTOMER_ID","DESTINATION_ID","CLIENT_ID","SITE_TYPE","RAW_USER_AGENT_STRING","ENCRYPTED_DISCOUNT_HASH","DEVICE_TYPE","URL","REDIRECT_TYPE"};
        List<Tuple> results = flowResultsMap(flow);
        //first tuple test
        Tuple t1 = results.get(0);
        assertEquals(t1.size(),26);
        assertEquals("6257994064607753395", t1.get(0).toString()); //searchid
        assertEquals(new String("2016-03-10T09:25:39-08:00"),t1.get(1).toString()); //
        assertEquals(new String("68.184.77.220"),t1.get(25).toString()); //ip address
        assertEquals(new String("32.38315963745117"),t1.get(22).toString()); //lat
        assertEquals(new String("-86.28196716308594"),t1.get(23).toString()); //long
        assertEquals(new String("montgomery"),t1.get(20).toString()); //city
        assertEquals(new String("al"),t1.get(21).toString()); //state
        assertEquals(new String("US"),t1.get(17).toString()); //country
        assertNull(t1.get(8));
        //assertEquals(new String(" "),t1.get(8).toString());
        assertEquals("https://www.qa.hotwire.com/Hotel-Search?sid=B378435|||tmid=18028592452|||paandi=true|||bid=S270",t1.get(14).toString());

        Tuple t2 = results.get(1);
        assertEquals(t2.size(),26);
        assertEquals("6257994064607753394", t2.get(0).toString()); //searchid
        assertNull(t2.get(22));//lat
        assertNull(t2.get(23));//long
        //assertEquals(new String(" "),t2.get(22).toString()); //lat
        //assertEquals(new String(" "),t2.get(23).toString()); //long
        assertEquals("https://www.qa.hotwire.com/Hotel-Search?sid=B378435|||tmid=18028592452|||paandi=true|||bid=S270",t2.get(14).toString());

        Tuple t3 = results.get(2);
        assertEquals(t3.size(),26);
        assertEquals("6257994064607753393", t3.get(0).toString()); //searchid
        assertNull(t3.get(22)); //lat
        assertNull(t3.get(23)); //long
        //assertEquals(new String(" "),t3.get(22).toString()); //lat
        //assertEquals(new String(" "),t3.get(23).toString()); //long
        assertNotNull(t3.get(21).toString()); //not null state
        assertEquals(new String("al"),t3.get(21).toString()); //state
        assertEquals("https://www.qa.hotwire.com/Hotel-Search?sid=B378435|||tmid=18028592452|||paandi=true|||bid=S270",t3.get(14).toString());

    }
    
    String createTempFile(String contents, String name) throws FileNotFoundException, UnsupportedEncodingException {
    	File f = new File(name);
    	
    	PrintWriter writer = new PrintWriter(new File(name), "UTF-8");
    	writer.println(contents);
    	writer.close();
    	
    	return f.getAbsolutePath();
    	
    }
    
    List flowResultsMap(Flow flow) throws IOException {
    	TupleEntryIterator iterator = flow.openSink();
    	List<Tuple> result = new ArrayList<Tuple>();
       
    	int i =0;
        
    	while (iterator.hasNext()){
        	TupleEntry entry = iterator.next();
        	Tuple t = new Tuple();
        	t.addAll(entry.getTuple());
        	System.out.println(t.toString("@~@", true));
        	result.add(t);
        }
 
        return result;
    }
    
}