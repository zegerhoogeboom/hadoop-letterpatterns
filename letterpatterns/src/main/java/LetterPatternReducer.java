import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

class LetterPatternReducer extends Reducer<Text, LetterKeyValue, Text, Text>
{
	Table<String, String, Integer> table;
	String combined;
	int total;

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
		context.write(key, new Text(String.format("%s - Total: %s", combined, total)));
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
			for (Map.Entry<String, Integer> rowKey : rows.getValue().entrySet()) {
				combined += new Text(rowKey.getKey()+":"+rowKey.getValue()).toString() + " ";
				total += rowKey.getValue();
			}
		}
	}
}