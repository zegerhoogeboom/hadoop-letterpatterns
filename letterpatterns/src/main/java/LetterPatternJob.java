import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Zeger Hoogeboom
 */
public class LetterPatternJob extends Configured implements Tool
{
	public int run(String[] args) throws Exception
	{
		Job job = Job.getInstance(getConf());
		job.setJarByClass(LetterPatternJob.class);
		job.setJobName(getClass().getSimpleName());

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(LetterPatternMapper.class);
//		job.setPartitionerClass(KeyPartitioner.class);
		job.setReducerClass(LetterPatternReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LetterKeyValue.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception
	{
		int rc = ToolRunner.run(new LetterPatternJob(), args);
		System.exit(rc);
	}
}