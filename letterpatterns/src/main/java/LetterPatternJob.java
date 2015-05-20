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
	int currentLetter = 0;
	String alphabet = "abcdefghijklmnopqrstuvwxyz";
	Map<String, Integer> reverseTotals = new TreeMap<>();
	Formatter formatter = new Formatter();
	BufferedWriter writer;

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
		LetterPatternJob job = new LetterPatternJob();
		int rc = ToolRunner.run(job, args);

		FileInputStream fstream = new FileInputStream("output/part-r-00000"); //should be reading any file like part-r-xxxxx
		BufferedReader reader = new BufferedReader(new InputStreamReader(fstream));
		FileOutputStream fos = new FileOutputStream("output/final.txt");
		job.writer = new BufferedWriter(new OutputStreamWriter(fos));

		job.setupHeader();

		String line;
		while ((line = reader.readLine()) != null) {
			job.processLine(line);
		}

		job.setupBottom();

		job.writer.close();
		reader.close();
		System.exit(rc);
	}

	private void setupHeader() throws IOException
	{
		String header = this.formatter.format("%8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s",
				"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z").toString();
		writer.write(header);
		writer.newLine();
	}

	private void setupBottom() throws IOException
	{
		this.formatter = new Formatter();
		for (Map.Entry<String, Integer> entry : this.reverseTotals.entrySet()) {
			this.formatter = this.formatter.format("%8s", entry.getKey() + ":" + entry.getValue());
		}
		writer.write(this.formatter.toString());
	}

	private void processLine(String line) throws IOException
	{
		formatter = new Formatter();
		String key = line.split("\t")[0];
		String value = line.split("\t")[1];
		String[] split = value.split(",");

		formatter = formatter.format("%1s", key);
		this.currentLetter = 0;
		for (String s : split) {
			formatter = this.processKeyValueString(s);
		}

		writer.write(formatter.toString());
		writer.newLine();
	}

	private Formatter processKeyValueString(String toBeSplit)
	{
		String[] keyValuePair = toBeSplit.split(":");
		String columnKey = keyValuePair[0];
		if (keyValuePair.length == 1) {  //total isn't split by ":"
			formatter = formatter.format("%8d", Integer.parseInt(columnKey));
			return formatter;
		}
		int columnValue = Integer.parseInt(keyValuePair[1]);
		formatter = this.processKeyValuePair(columnKey, columnValue);
		return formatter;
	}

	private Formatter processKeyValuePair(String key, int value)
	{
		int letterIndex = alphabet.indexOf(key);
		formatter = this.setupZeros(letterIndex);
		Integer totalForLetter = reverseTotals.get(key);
		if (totalForLetter == null) totalForLetter = 0;
		reverseTotals.put(key, totalForLetter + value);
		formatter = formatter.format("%8d", value);
		return formatter;
	}

	private Formatter setupZeros(int letterIndex)
	{
		this.currentLetter++;
		if (letterIndex != (currentLetter - 1)) { //always increase the currentletter, just compare the equals with the previous value
			formatter = formatter.format("%8d", 0);
			return setupZeros(letterIndex);
		}
		return formatter;
	}
}