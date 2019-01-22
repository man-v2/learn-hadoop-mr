package cn.com.weekpark.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordCount extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{

	private static final int MISSING = 9999;
	
	@Override
	public void map(LongWritable eky, Text value, OutputCollector<Text, IntWritable> arg2, Reporter arg3)
			throws IOException {
		String line = value.toString();
		String year = line.substring(15, 19);
		int airTemperature;
		if(line.charAt(78) == '+') {
			airTemperature = Integer.parseInt(line.substring(88,92));
		}else {
			airTemperature = Integer.parseInt(line.substring(87,92));
		}
		String quality = line.substring(92,93);
		if(airTemperature != MISSING && quality.matches("01459")) {
			
		}
	}
	
}
