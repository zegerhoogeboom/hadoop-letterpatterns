import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

class LetterPatternReducer extends Reducer<Text, LetterKeyValue, Text, Text>
{
	final static Logger logger = LoggerFactory.getLogger(LetterPatternReducer.class);
	Table<String, String, Integer> table;
	String combined;
	int total;
	List<String> processedStartingLetters = new ArrayList<String>();
	boolean newStartingLetter = false;
	String alphabet = "abcdefghijklmnopqrstuvwxyz";

	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		resetData();
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		resetData();
	}

	@Override
	protected void reduce(Text key, Iterable<LetterKeyValue> values, Context context) throws IOException, InterruptedException
	{
		setupRows(key, values);
		setupOutput();
		context.write(key, new Text(combined));
		resetData();
	}

	private void resetData()
	{
		table = TreeBasedTable.create();
		combined = "";
		total = 0;
	}

	private void setupRows(Text key, Iterable<LetterKeyValue> values)
	{
		String firstLetter = key.toString().substring(0, 1);
		if (processedStartingLetters.contains(firstLetter) == false) {
			processedStartingLetters.add(firstLetter);
			newStartingLetter = true;
		}
		for (LetterKeyValue next : values) {
			Integer value = 1;
			if (table.contains(key.toString(), next.getKey())) {
				value += table.get(key.toString(), next.getKey());
			}
			table.put(key.toString(), next.getKey(), value);
		}
	}

	private void setupOutput()
	{
		for (Map.Entry<String, Map<String, Integer>> rows : table.rowMap().entrySet()) {
			int lastRowKeyIndex = -1; // -1 to insert an extra tab if there's no match with "a"
			for (Iterator<Map.Entry<String, Integer>> it = rows.getValue().entrySet().iterator(); it.hasNext(); ) {
				Map.Entry<String, Integer> rowKey = it.next();
				int currentRowKeyIndex = alphabet.indexOf(rowKey.getKey());
				String key = String.format("%s%s:%s\t",
						getTabs(lastRowKeyIndex, currentRowKeyIndex),
						rowKey.getKey(),
						rowKey.getValue()
				);
				combined += key;
				total += rowKey.getValue();
				lastRowKeyIndex = currentRowKeyIndex;
				if (!it.hasNext()) {
					logger.info("Last row: "+lastRowKeyIndex +"    z+1 index:" +(alphabet.indexOf("z")+1));
					combined += String.format("%s%s%s", combined, getTabs(lastRowKeyIndex, alphabet.indexOf("z")+1), total);
				}
			}
		}
	}

	private String getTabs(int lastRowKeyIndex, int currentRowKeyIndex) {
		String tabs = "";
		int difference = currentRowKeyIndex - lastRowKeyIndex;
		logger.info("Difference: "+String.valueOf(difference));
		if (difference > 0) {
			for (int i = 1; i < difference; i++) {
				tabs = String.format("\t%s", tabs);
			}
		}
		return tabs;
	}
}