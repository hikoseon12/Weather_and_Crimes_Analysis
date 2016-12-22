import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SeasonFindMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	private final static IntWritable outputValue = new IntWritable(1);
	
	private Text outputKey = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		if(key.get() > 0){
			String[] colums = value.toString().split(",");
			if(colums != null && colums.length > 0){
				try{
					outputKey.set(colums[5]);
					String month = colums[2].substring(0,2);
					if(month.equals("01") || month.equals("02")){
						outputKey.set("summer_" + colums[5]);
						context.write(outputKey, outputValue);
					}
					else if(month.equals("07") || month.equals("08")){
						outputKey.set("winter_" + colums[5]);
						context.write(outputKey, outputValue);
					}

				}catch(Exception e){
					e.printStackTrace();

				}
			}
		}
	}
}


