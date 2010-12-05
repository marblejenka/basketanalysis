package com.mabrlejenka.basketanalysis.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.mabrlejenka.basketanalysis.utils.Constants;

public class BasketAnalysisFilterdIntermediateKey implements
		WritableComparable<BasketAnalysisFilterdIntermediateKey> {

	Text user = new Text();
	LongWritable unixDateTime = new LongWritable();

	public BasketAnalysisFilterdIntermediateKey() {
	}

	public BasketAnalysisFilterdIntermediateKey(String user, long unixDateTime) {
		set(user, unixDateTime);
	}

	public void set(String user, long unixDateTime) {
		this.user.set(user);
		this.unixDateTime.set(unixDateTime);
	}

	public Text getUser() {
		return user;
	}

	public LongWritable getUnixDateTime() {
		return unixDateTime;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.user.write(out);
		this.unixDateTime.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.user.readFields(in);
		this.unixDateTime.readFields(in);
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(user.toString()).append(Constants.TEXT_DELIMITER);
		builder.append(unixDateTime.get());
		return builder.toString();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}

		if (!(obj instanceof BasketAnalysisFilterdIntermediateKey)) {
			return false;
		}
		
		BasketAnalysisFilterdIntermediateKey other = (BasketAnalysisFilterdIntermediateKey) obj;

		return this.user.equals(other.user) && this.unixDateTime.equals(other.unixDateTime);
	}
	
	@Override
	public int hashCode() {
		return this.user.hashCode() * this.unixDateTime.hashCode() * Constants.HASH_MAGIC_NUMBER;
	}

	@Override
	public int compareTo(BasketAnalysisFilterdIntermediateKey other) {
		int result = 0;
		result = this.user.toString().compareTo(other.user.toString());
		if (result != 0) {
			return result;
		}
		result = this.unixDateTime.compareTo(other.unixDateTime);
		if (result != 0) {
			return result;
		}

		return 0;
	}
}
