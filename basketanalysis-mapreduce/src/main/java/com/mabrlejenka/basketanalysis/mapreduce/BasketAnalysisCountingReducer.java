package com.mabrlejenka.basketanalysis.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.mabrlejenka.basketanalysis.support.BasketAnalysisCountingSupport;
import com.mabrlejenka.basketanalysis.utils.Constants;
import com.mabrlejenka.basketanalysis.writables.BasketAnalysisFilterdIntermediateKey;
import com.mabrlejenka.basketanalysis.writables.BasketAnalysisFilterdIntermediateValue;

/**
 * 
 * in
 * 
 * 
 * @author marblejenka
 * 
 */
public class BasketAnalysisCountingReducer
		extends
		Reducer<BasketAnalysisFilterdIntermediateKey, BasketAnalysisFilterdIntermediateValue, LongWritable, Text> {
	LongWritable emitKey = new LongWritable();
	Text emitValue = new Text();

	protected void reduce(
			BasketAnalysisFilterdIntermediateKey key,
			java.lang.Iterable<BasketAnalysisFilterdIntermediateValue> values,
			org.apache.hadoop.mapreduce.Reducer<BasketAnalysisFilterdIntermediateKey, BasketAnalysisFilterdIntermediateValue, LongWritable, Text>.Context context)
			throws java.io.IOException, InterruptedException {
		BasketAnalysisCountingSupport counter = new BasketAnalysisCountingSupport();

		for (BasketAnalysisFilterdIntermediateValue value : values) {
			long process = counter.process(value);
			
			emitKey.set(process);
			emitValue.set(buildValue(null, value.getWord().toString()));
			context.write(emitKey, emitValue);
		}
	};
	
	String buildValue(String base, String trans) {
		return base + Constants.TEXT_DELIMITER + trans;
	}
}
