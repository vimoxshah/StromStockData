package org.vimox.storm;


import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class YFinanceTopology {

	public static void main(String[] args) throws InterruptedException, IOException {
		// TODO Auto-generated method stub
		
		
		List<String> l = new ArrayList<String>();
		
		
		System.out.println("Enter ur stock");
		
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String st = br.readLine();
		
		String[] tokens = st.split(",");
		
		for(int i=0;i<tokens.length;i++){
			l.add(tokens[i]);
		}
		

		FileOutputStream fos= new FileOutputStream("myfile.txt");
        ObjectOutputStream oos= new ObjectOutputStream(fos);
		oos.writeObject(l);
		oos.close();
		fos.close();
		//SerializeUtil.serialize(yd, "serialization.txt");
		
		
		
		
		Config conf = new Config();
		conf.setDebug(true);
		
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("yfinance-spout", new YFiananceSpout());
		builder.setBolt("yfinance-bolt", new YFinanceBolt()).fieldsGrouping("yfinance-spout", new Fields("company"));
		
		LocalCluster lc = new LocalCluster();
		lc.submitTopology("YFinanceTopology", conf, builder.createTopology());
		
		

	}

}
