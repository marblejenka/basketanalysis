package com.mabrlejenka.basketanalysis.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.mabrlejenka.basketanalysis.writables.BasketAnalysisFilterdIntermediateKey;
import com.mabrlejenka.basketanalysis.writables.BasketAnalysisFilterdIntermediateValue;
import com.mabrlejenka.basketanalysis.writables.BasketAnalysisInputValue;

public class BasketAnalysisFilterMapper
		extends
		Mapper<LongWritable, BasketAnalysisInputValue, BasketAnalysisFilterdIntermediateKey, BasketAnalysisFilterdIntermediateValue> {
	BasketAnalysisFilterdIntermediateKey emitKey = new BasketAnalysisFilterdIntermediateKey();
	BasketAnalysisFilterdIntermediateValue emitValue = new BasketAnalysisFilterdIntermediateValue();

	protected void map(
			LongWritable key,
			BasketAnalysisInputValue value,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, BasketAnalysisInputValue, BasketAnalysisFilterdIntermediateKey, BasketAnalysisFilterdIntermediateValue>.Context context)
			throws java.io.IOException, InterruptedException {
		// filter 
		
		
		context.write(emitKey, emitValue);
	};

}
