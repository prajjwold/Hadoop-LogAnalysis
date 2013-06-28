package com.hadoop.project.LogAnalysis;


import static org.fest.assertions.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;


/**
 * Unit test for simple App.
 */

/*----------------input-------------------------------------------------------------
SN^USERID^USERNAME^APPID^CLIENTID^CLIENTNAME^FRONTID^PAGETITLE^EXECTIME^PRESENTTIME^FORMLOADTIME
81^81^Navin Gorkhali^149-001^149^Health Plans Inc (HPI)^Hea200605^S001^4^37^19-JUN-06
82^81^Navin Gorkhali^149-001^149^Health Plans Inc (HPI)^Hea200605^543^6^9^19-JUN-06
83^81^Navin Gorkhali^149-001^149^Health Plans Inc (HPI)^Hea200605^S001^3^6^19-JUN-06
109^7847^Smriti Shrestha^108-001^108^Medicare y Mucho Mas^Med200605^S001^21^68^19-JUN-06
110^7847^Smriti Shrestha^108-001^108^Medicare y Mucho Mas^Med200605^S003^3^3^19-JUN-06
111^7847^Smriti Shrestha^108-001^108^Medicare y Mucho Mas^Med200605^S003^3^3^19-JUN-06
210^6379^Bijay Karki^279-555^279^D2Hawkeye Demo v4.5^D2H200606^L02^463^1244^19-JUN-06
211^7924^Sakar Shrestha^108-001^108^Medicare y Mucho Mas^Med200605^301^92^3595^19-JUN-06
987^5505^Thakur Gyawali^149-001^149^Health Plans Inc (HPI)^Hea200605^S001^5^48^19-JUN-06
1144^5911^Stephen Gilbert^031-001^031^JSA Healthcare Inc.^JSA200606^115D3^9^28^20-JUN-06
1145^5911^Stephen Gilbert^031-001^031^JSA Healthcare Inc.^JSA200606^113D^166^177^20-JUN-06
1146^000414^Carol Dillard^011-010^011^EPOCH - LCM^EPO200605^949^305^1341^20-JUN-06
*/

public class LogAnalysisTest 
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
	
	  MapDriver<Text, Text, Text, LongWritable> mapDriver;
	  ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
	  MapReduceDriver<Text, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;

       @Before
    public void setUp() {
      TokenizerMapper mapper = new TokenizerMapper();
      LongSumReducer reducer = new LongSumReducer();
      mapDriver = MapDriver.newMapDriver(mapper);;
      reduceDriver = ReduceDriver.newReduceDriver(reducer);
      mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }
   
       @Test
    public void testMapper() throws IOException {
      mapDriver.withInput(new Text(), new Text(
          "81^81^Navin Gorkhali^149-001^149^Health Plans Inc (HPI)^Hea200605^S001^4^37^19-JUN-06"));
//      mapDriver.withOutput(new Text("81"), new LongWritable(1));
//      mapDriver.runTest();
      final List<Pair<Text, LongWritable>> result = mapDriver.run();
      
      final Pair<Text, LongWritable> str1 = new Pair<Text, LongWritable>(new Text("81"), new LongWritable(1));
      final Pair<Text, LongWritable> str2 = new Pair<Text, LongWritable>(new Text("Navin Gorkhali"), new LongWritable(1));

       assertThat(result).isNotNull().hasSize(11).containsExactly(str1, str2);
    }
   
       @Test
    public void testReducer() throws IOException {
      List<LongWritable> values = new ArrayList<LongWritable>();
      values.add(new LongWritable(1));
      values.add(new LongWritable(1));
      reduceDriver.withInput(new Text("81"), values);
      reduceDriver.withOutput(new Text("81"), new LongWritable(2));
      reduceDriver.runTest();
    }
}
