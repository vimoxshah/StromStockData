package org.vimox.storm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import yahoofinance.histquotes.HistoricalQuote;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class YFinanceBolt implements IRichBolt {
	
	OutputCollector collector;
	List<HistoricalQuote> l1;
	
	Map<String,Integer> dvalue ;
	Map<String,Boolean> fvalue;
	
	
	List<String> l = new ArrayList<String>();
	

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		dvalue = new HashMap<String,Integer>();
		try {
			

	         FileInputStream fis = new FileInputStream("myfile.txt");
	         ObjectInputStream ois = new ObjectInputStream(fis);
	         l = (ArrayList) ois.readObject();
	         ois.close();
	         fis.close();
			
	         for(String tmp:l){
	        	 dvalue.put(tmp, 500);
	         }
	         
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	//	dvalue.put("GOOGL", 700);
		
		fvalue=new HashMap<String,Boolean>();
		
		 
		this.collector=collector;
		
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple tuple) {
		
		String company = tuple.getString(0);
		Double price = tuple.getDouble(1);
		l1 = (List<HistoricalQuote>) (tuple.getValue(2));
		OutputStream out1;
		try {
			out1 = new FileOutputStream("output1.txt",true);
			out1.write(l1.toString().getBytes());
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		if(dvalue.containsKey(company)){
			int dprice = dvalue.get(company);
			
			if(price<dprice){
				fvalue.put(company, true);
			}
			else{
				fvalue.put(company, false);
			}
		}
		
		collector.ack(tuple);
	}

	@Override
	public void cleanup() {
		 try {
			OutputStream out = new FileOutputStream("output.txt",true);
			
			for(Map.Entry<String,Boolean> entryset:fvalue.entrySet()){
				 String ms=(entryset.getKey()+":  "+entryset.getValue());
				 out.write(ms.getBytes());
				}
			out.close();
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		//System.out.println(l);
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields("decided price"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
