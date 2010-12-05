package com.mabrlejenka.basketanalysis.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.mabrlejenka.basketanalysis.support.BasketAnalysisCountingSupport;
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
		Reducer<BasketAnalysisFilterdIntermediateKey, BasketAnalysisFilterdIntermediateValue, BasketAnalysisFilterdIntermediateValue, LongWritable> {
	BasketAnalysisFilterdIntermediateKey emitKey = new BasketAnalysisFilterdIntermediateKey();
	BasketAnalysisFilterdIntermediateValue emitValue = new BasketAnalysisFilterdIntermediateValue();

	protected void reduce(
			BasketAnalysisFilterdIntermediateKey key,
			java.lang.Iterable<BasketAnalysisFilterdIntermediateValue> values,
			org.apache.hadoop.mapreduce.Reducer<BasketAnalysisFilterdIntermediateKey, BasketAnalysisFilterdIntermediateValue, BasketAnalysisFilterdIntermediateValue, LongWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		BasketAnalysisCountingSupport counter = new BasketAnalysisCountingSupport();
		
		for (BasketAnalysisFilterdIntermediateValue value : values) {
			counter.add(value);
		}
	};
}
