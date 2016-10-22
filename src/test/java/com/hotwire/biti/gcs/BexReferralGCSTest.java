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

public class BexReferralGCSTest extends CascadingTestCase{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

    public BexReferralGCSTest () {
        super( "distinct locations count tests" );
    }
    
    public void testData() throws IOException  {
    	String bexReferral =  "145\t840785591092328720@~@O@~@B378436@~@S270@~@@~@@~@@~@2016-03-10T09:24:15-08:00@~@@~@@~@@~@@~@@~@https://www.qa.hotwire.com/hotels/@~@true\n"  //empty referral
    						 + "146\t79246608584342@~@O@~@S132@~@B132@~@null@~@null@~@null@~@2016-03-20T02:58:18-07:00@~@null@~@123@~@null@~@null@~@null@~@123@~@false\n"  //opm referral
    						+ "146\t79246608584343@~@N@~@N-HFA-502@~@D1476@~@V-HFA-502-V1SS@~@null@~@null@~@2016-03-20T02:58:18-07:00@~@null@~@123@~@null@~@null@~@null@~@https://jslave25.hotwire.com:7002/?vt.AWA14=2&vt.ALS14=2&vt.RTF15=1@~@true";  //dbm referral
    	Fields bex_fields = new Fields( "line" );
    	String s3outputPath = "/Users/srathi/Documents/Architecture/GCS/gcs_s3_out";
    	String dboutputPath = "/Users/srathi/Documents/Architecture/GCS/gcs_db_out";
        //Tap users = new FileTap(new TextDelimited(bex_fields,false,"@~@"), createTempFile(bexReferral, "users_test.txt"));;
    	Tap referrals = new FileTap(new TextDelimited(new Fields("offset","line")), createTempFile(bexReferral, "referrals.txt"));
        Tap s3out = new FileTap(new TextDelimited(false, "@~@"), "target/s3_referral.txt",SinkMode.REPLACE);
        Tap dbout = new FileTap(new TextDelimited(false, "@~@"), "target/db_referral.txt",SinkMode.REPLACE);

        Properties properties = new Properties();
  	    AppProps.setApplicationJarClass( properties, BexReferralGCSTest.class );
  	    FlowConnector flowConnector = new LocalFlowConnector(properties);
          
        FlowDef flowDef = BexReferralGCS.createWorkflow(referrals, s3out,dbout);
        Flow flow = flowConnector.connect( flowDef );
        flow.complete();
        validateLength(flow, 3);
        //"SEARCH_ID","REFERRAL_TYPE","REFERRER_ID","LINK_ID","VERSION_ID","KEYWORD_ID","MATCH_TYPE_ID","REFERRAL_DATE","REFERRAL_SITE_ID","AFFILIATE_NETWORK_ID","KEYWORD_ACCOUNT_ID","KEYWORD_CAMPAIGN_ID","REFERRING_HOTEL_ID","SEARCH_ENGINE");
        List<Tuple> results = flowResultsMap(flow);
        //first tuple test
        Tuple t1 = results.get(0);
        assertEquals(t1.size(),15);
        assertEquals("840785591092328720", t1.get(0).toString()); //searchid
        assertEquals(new String("2016-03-10T09:24:15-08:00"),t1.get(7).toString()); //
        assertEquals("https://www.qa.hotwire.com/hotels/",t1.get(13));
        assertNull(t1.get(8));
        //second tuple tests
        Tuple t2 = results.get(1);
        assertEquals(t2.size(),15);
        assertEquals("79246608584342", t2.get(0).toString()); //searchid
        assertEquals("O",t2.get(1).toString()); // referral type
        assertEquals("S132",t2.get(2).toString()); //sid
        assertEquals("B132",t2.get(3).toString()); //sid
        //third tuple tests
        Tuple t3 = results.get(2);
        assertEquals(t3.size(),15);
        assertEquals("79246608584343", t3.get(0).toString()); //searchid
        assertEquals("N",t3.get(1).toString()); // referral type
        assertEquals("N-HFA-502",t3.get(2).toString()); //nid
        assertEquals("D1476",t3.get(3).toString()); //did
        assertEquals("V-HFA-502-V1SS",t3.get(4).toString()); //vid
        assertEquals("https://jslave25.hotwire.com:7002/?vt.AWA14=2|||vt.ALS14=2|||vt.RTF15=1",t3.get(13));
        assertEquals("true", t3.get(14));
        
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