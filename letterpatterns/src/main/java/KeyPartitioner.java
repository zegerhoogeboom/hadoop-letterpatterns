import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Zeger Hoogeboom
 */
public class KeyPartitioner extends Partitioner<Text, LetterKeyValue>
{

	@Override
	public int getPartition(Text text, LetterKeyValue letterKeyValue, int numPartitions)
	{
		char key = text.toString().charAt(0);
		int ascii = (int) key;
		return (ascii - 32) % numPartitions;
	}
}
