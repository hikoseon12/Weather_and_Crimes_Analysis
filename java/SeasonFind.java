import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SeasonFind {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		// 입력출 데이터 경로 확인 
		if (args.length != 2) {
			System.err.println("Usage: SeasonFind <input> <output>");
			System.exit(2);
		}
		// Job 이름 설정
		Job job = Job.getInstance(conf, "SeasonFind");

		// 입출력 데이터 경로 설정
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Job 클래스 설정
		job.setJarByClass(SeasonFind.class);
		// Mapper 클래스 설정
		job.setMapperClass(SeasonFindMapper.class);
		// Reducer 클래스 설정
		job.setReducerClass(SeasonFindReducer.class);

		// 입출력 데이터 포맷 설정
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// 출력키 및 출력값 유형 설정
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
	}
}



