import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Formatter;
import java.util.Map;
import java.util.TreeMap;

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

	final static Logger logger = LoggerFactory.getLogger(LetterPatternJob.class);

	public static void main(String[] args) throws Exception
	{
		int rc = ToolRunner.run(new LetterPatternJob(), args);

		FileInputStream fstream = new FileInputStream("output/part-r-00000"); //should be reading any file like part-r-xxxxx
		BufferedReader reader = new BufferedReader(new InputStreamReader(fstream));

		FileOutputStream fos = new FileOutputStream("output/final.txt");
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos));

		String alphabet = "abcdefghijklmnopqrstuvwxyz";
		String line;
		Formatter formatter = new Formatter();
		String header = formatter.format("%8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s",
				"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z").toString();
		writer.write(header);
		writer.newLine();

		Map<String, Integer> reverseTotals = new TreeMap<>();

		while ((line = reader.readLine()) != null) {
			formatter = new Formatter();
			String key = line.split("\t")[0];
			String value = line.split("\t")[1];
			String[] split = value.split(",");

			formatter = formatter.format("%1s", key);
			int currentLetter = 0;
			int columnValue = 0;
			for (String s : split) {
				String[] keyValuePair = s.split(":");
				String columnKey = keyValuePair[0];
				if (keyValuePair.length == 1) {
					formatter = formatter.format("%8d", Integer.parseInt(columnKey));
					break;
				}
				columnValue = Integer.parseInt(keyValuePair[1]);
				int letterIndex = alphabet.indexOf(columnKey);
				while (letterIndex != currentLetter) {
					formatter = formatter.format("%8d", 0);
					currentLetter++;
					if (letterIndex != currentLetter || letterIndex == 0) {
					}
				}

				Integer totalForLetter = reverseTotals.get(columnKey);
				if (totalForLetter == null) totalForLetter = 0;
				reverseTotals.put(columnKey, totalForLetter + columnValue);
				formatter = formatter.format("%8d", columnValue);
			}
			writer.write(formatter.toString());
			writer.newLine();
		}

		formatter = new Formatter();
		for (Map.Entry<String, Integer> entry : reverseTotals.entrySet()) {
			formatter = formatter.format("%8s", entry.getKey() + ":" + entry.getValue());
		}

		writer.write(formatter.toString());

		writer.close();
		reader.close();
		System.exit(rc);
	}
}