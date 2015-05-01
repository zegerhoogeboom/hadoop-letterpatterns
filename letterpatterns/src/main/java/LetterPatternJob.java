import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

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

class LetterPatternReducer extends Reducer<Text, LetterKeyValue, Text, Text>
{
	Table<String, String, Integer> table;
	String combined;
	int total;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		table = TreeBasedTable.create();
		combined = "";
		total = 0;
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		setup(context);
	}

	@Override
	protected void reduce(Text key, Iterable<LetterKeyValue> values, Context context) throws IOException, InterruptedException
	{
		setupRows(key, values);
		setupOutput();
		context.write(key, new Text(String.format("%s - Total: %s", combined, total)));
		cleanup(context);
	}

	private void setupRows(Text key, Iterable<LetterKeyValue> values)
	{
		for (LetterKeyValue next : values) {
			Integer value = 1;
			if (table.contains(key.toString(), next.getKey())) {
				value = next.increaseAmount();
			}
			table.put(key.toString(), next.getKey(), value);
		}
	}

	private void setupOutput()
	{
		for (Map.Entry<String, Map<String, Integer>> rows : table.rowMap().entrySet()) {
			for (Map.Entry<String, Integer> rowKey : rows.getValue().entrySet()) {
				combined += new Text(rowKey.getKey()+":"+rowKey.getValue()).toString() + " ";
				total += rowKey.getValue();
			}
		}
	}
}

class LetterPatternMapper extends Mapper<LongWritable, Text, Text, LetterKeyValue>
{
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String[] split = formatText(value);
		for (String word : split) {
			for (int letterIndex = 0; letterIndex < word.length() - 1; letterIndex++) {
				context.write(
						new Text(String.valueOf(word.charAt(letterIndex))),
						new LetterKeyValue(String.valueOf(word.charAt(letterIndex + 1)), 1)
				);
			}
		}
	}

	private String[] formatText(Text value)
	{
		return value.toString()
				.toLowerCase()
				.replaceAll("[^A-Za-z ]", "")
				.split("[\\W]");
	}
}