package com.mabrlejenka.basketanalysis.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.mabrlejenka.basketanalysis.utils.Constants;

public class BasketAnalysisFilterdIntermediateValue implements Writable {

	Text word = new Text();
	LongWritable unixDateTime = new LongWritable();

	public BasketAnalysisFilterdIntermediateValue() {
	}

	public BasketAnalysisFilterdIntermediateValue(String word, long unixDateTime) {
		set(word, unixDateTime);
	}

	public void set(String word, long unixDateTime) {
		this.word.set(word);
		this.unixDateTime.set(unixDateTime);
	}

	public Text getWord() {
		return word;
	}

	public LongWritable getUnixDateTime() {
		return unixDateTime;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.word.write(out);
		this.unixDateTime.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.word.readFields(in);
		this.unixDateTime.readFields(in);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(word.toString()).append(Constants.TEXT_DELIMITER);
		builder.append(unixDateTime.get());
		return builder.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}

		if (!(obj instanceof BasketAnalysisFilterdIntermediateValue)) {
			return false;
		}

		BasketAnalysisFilterdIntermediateValue other = (BasketAnalysisFilterdIntermediateValue) obj;

		return this.word.equals(other.word)
				&& this.unixDateTime.equals(other.unixDateTime);
	}

	@Override
	public int hashCode() {
		return this.word.hashCode() * this.unixDateTime.hashCode()
				* Constants.HASH_MAGIC_NUMBER;
	}
}
