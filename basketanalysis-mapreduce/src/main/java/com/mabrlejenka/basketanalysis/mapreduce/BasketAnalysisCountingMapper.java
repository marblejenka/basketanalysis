package com.mabrlejenka.basketanalysis.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.mabrlejenka.basketanalysis.utils.Constants;
import com.mabrlejenka.basketanalysis.writables.BasketAnalysisFilterdIntermediateKey;
import com.mabrlejenka.basketanalysis.writables.BasketAnalysisFilterdIntermediateValue;
import com.mabrlejenka.basketanalysis.writables.BasketAnalysisInputValue;

/**
 * 
 * in
 * 
 * 
 * @author marblejenka
 * 
 */
public class BasketAnalysisCountingMapper
		extends
		Mapper<LongWritable, Text, BasketAnalysisFilterdIntermediateKey, BasketAnalysisFilterdIntermediateValue> {
	BasketAnalysisFilterdIntermediateKey emitKey = new BasketAnalysisFilterdIntermediateKey();
	BasketAnalysisFilterdIntermediateValue emitValue = new BasketAnalysisFilterdIntermediateValue();

	protected void map(
			LongWritable key,
			Text value,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, BasketAnalysisInputValue, BasketAnalysisFilterdIntermediateKey, BasketAnalysisFilterdIntermediateValue>.Context context)
			throws java.io.IOException, InterruptedException {
		String[] split = value.toString().split(Constants.TEXT_DELIMITER);
		if (split.length != 3) {
			// filter if the input value length is not expected
			return;
		}
		
		String user = split[0];
		long unixDateTime = Long.parseLong(split[1]);
		String[] words = split[2].split(" ");
		
		for (String word : words) {
			emitKey.set(user, unixDateTime);
			emitValue.set(word, unixDateTime);

			context.write(emitKey, emitValue);
		}
	};
}
