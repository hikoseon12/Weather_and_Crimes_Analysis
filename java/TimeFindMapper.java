import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TimeFindMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	private final static IntWritable outputValue = new IntWritable(1);
	
	private Text outputKey = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		if(key.get() > 0){
			String[] colums = value.toString().split(",");
			if(colums != null && colums.length > 0){
				try{
					String time = colums[2].substring(11, 13);
					String ampm = colums[2].substring(20,22);
					if(time.equals("12") && ampm.equals("AM")){
						outputKey.set("00AM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("01") && ampm.equals("AM")){
						outputKey.set("01AM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("02") && ampm.equals("AM")){
						outputKey.set("02AM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("03") && ampm.equals("AM")){
						outputKey.set("03AM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("04") && ampm.equals("AM")){
						outputKey.set("04AM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("05") && ampm.equals("AM")){
						outputKey.set("05AM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("06") && ampm.equals("AM")){
						outputKey.set("06AM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("07") && ampm.equals("AM")){
						outputKey.set("07AM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("08") && ampm.equals("AM")){
						outputKey.set("08AM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("09") && ampm.equals("AM")){
						outputKey.set("09AM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("10") && ampm.equals("AM")){
						outputKey.set("10AM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("11") && ampm.equals("AM")){
						outputKey.set("11AM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("12") && ampm.equals("PM")){
						outputKey.set("12PM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("01") && ampm.equals("PM")){
						outputKey.set("01PM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("02") && ampm.equals("PM")){
						outputKey.set("02PM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("03") && ampm.equals("PM")){
						outputKey.set("03PM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("04") && ampm.equals("PM")){
						outputKey.set("04PM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("05") && ampm.equals("PM")){
						outputKey.set("05PM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("06") && ampm.equals("PM")){
						outputKey.set("06PM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("07") && ampm.equals("PM")){
						outputKey.set("07PM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("08") && ampm.equals("PM")){
						outputKey.set("08PM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("09") && ampm.equals("PM")){
						outputKey.set("09PM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("10") && ampm.equals("PM")){
						outputKey.set("10PM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
					else if(time.equals("11") && ampm.equals("PM")){
						outputKey.set("11PM_"+colums[5]+",");
						context.write(outputKey, outputValue);
					}
				}catch(Exception e){
					e.printStackTrace();

				}
			}
		}
	}
}


