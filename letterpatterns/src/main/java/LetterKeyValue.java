import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Zeger Hoogeboom
 */
public class LetterKeyValue implements Writable, WritableComparable<LetterKeyValue>
{
	String key;
	int amount;

	public LetterKeyValue()
	{
	}

	public LetterKeyValue(String key, int amount)
	{
		this.key = key;
		this.amount = amount;
	}

	public String getKey()
	{
		return key;
	}

	public void setKey(String key)
	{
		this.key = key;
	}

	public int getAmount()
	{
		return amount;
	}

	public void setAmount(int amount)
	{
		this.amount = amount;
	}

	public int increaseAmount()
	{
		this.amount++;
		return this.amount;
	}

	public int compareTo(LetterKeyValue o)
	{
		return key.compareTo(o.key);
	}

	public void write(DataOutput out) throws IOException
	{
		out.writeUTF(key);
		out.writeInt(amount);
	}

	public void readFields(DataInput in) throws IOException
	{
	 	key = in.readUTF();
		amount = in.readInt();
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (! (o instanceof LetterKeyValue)) return false;

		LetterKeyValue that = (LetterKeyValue) o;

		return ! (key != null ? ! key.equals(that.key) : that.key != null);
	}

	@Override
	public int hashCode()
	{
		int result = key != null ? key.hashCode() : 0;
		result = 31 * result + amount;
		return result;
	}

	@Override
	public String toString() {
		return "["+ this.key + ":" + this.amount + "]";
	}
}
