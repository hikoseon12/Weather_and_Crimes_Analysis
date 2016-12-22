import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Scanner;

public class WeatherFindMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	private final static IntWritable outputValue = new IntWritable(1);
	
	private Text outputKey = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		if(key.get() > 0){
			String[] colums = value.toString().split(",");
			if(colums != null && colums.length > 0){
				try{
					outputKey.set(colums[1]);

					if(colums[22].contains("RA")){
						context.write(outputKey, outputValue);
					}

				}catch(Exception e){
					e.printStackTrace();

				}
			}
		}
	}
}


