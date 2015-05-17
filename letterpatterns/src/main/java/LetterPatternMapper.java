import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

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