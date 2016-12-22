import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;

public class MatchingWeatherandCrimesMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	private final static IntWritable outputValue = new IntWritable(1);
	private Text outputKey = new Text();
	private ArrayList<String> date = new ArrayList<String>();
	
	public void setup(Context context) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://master:9000");
		FileSystem dfs = FileSystem.get(conf);
		System.out.println("Home Path: " + dfs.getHomeDirectory());
		Path pt = new Path("weather/Weather_NS.csv");
		BufferedReader br=new BufferedReader(new InputStreamReader(dfs.open(pt)));

		String line;
		line = br.readLine();
		while (line != null){
			String[] day = line.toString().split("\t");
			String sub_day = day[0].substring(4,6) +'/'+ day[0].substring(6,8) +'/'+ day[0].substring(0,4);
			date.add(sub_day + " ");
		    	System.out.println(date);
			line = br.readLine();
		}
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		if(key.get() > 0){
			String[] colums = value.toString().split(",");
			if(colums != null && colums.length > 0){
				try{
					outputKey.set(colums[2]);
					if(date.contains(colums[2])){
						context.write(outputKey, outputValue);
					}
				}catch(Exception e){
					e.printStackTrace();
					System.out.println("error!!!");
				}
			}
		}
	}
}


