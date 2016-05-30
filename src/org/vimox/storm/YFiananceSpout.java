package org.vimox.storm;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import clojure.tools.logging.impl.Logger;
import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class YFiananceSpout implements IRichSpout {
	
	SpoutOutputCollector collector;
	TopologyContext context;
	
	List<String> l = new ArrayList<String>();
	

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector=collector;
		this.context=context;
		

		 try
	     {
	         FileInputStream fis = new FileInputStream("myfile.txt");
	         ObjectInputStream ois = new ObjectInputStream(fis);
	         l = (ArrayList) ois.readObject();
	         ois.close();
	         fis.close();
	      }
		 catch(IOException ioe){
	          ioe.printStackTrace();
	          return;
	       }
		 catch(ClassNotFoundException c){
	          System.out.println("Class not found");
	          c.printStackTrace();
	          return;
	       }
		
	}
	
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void nextTuple() {
		
		
		try {
			
			for(String tmp:l){
			Stock st = YahooFinance.get(tmp,true);
			
			BigDecimal price =st.getQuote().getPrice();
			
			
			collector.emit(new Values(tmp,price.doubleValue(),st.getHistory()));
			
			}
			/*st= YahooFinance.get("GOOGL",true);
			price=st.getQuote(true).getPrice();
			
			collector.emit(new Values("GOOGL",price.doubleValue(),st.getHistory()));*/
			
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("company","price","history"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	

}
