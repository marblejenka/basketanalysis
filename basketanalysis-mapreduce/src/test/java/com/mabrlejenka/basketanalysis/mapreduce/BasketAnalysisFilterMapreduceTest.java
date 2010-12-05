package com.mabrlejenka.basketanalysis.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriverFactory;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.mabrlejenka.basketanalysis.BasketAnalysis;
import com.mabrlejenka.basketanalysis.writables.BasketAnalysisFilterdIntermediateKey;
import com.mabrlejenka.basketanalysis.writables.BasketAnalysisFilterdIntermediateValue;
import com.mabrlejenka.basketanalysis.writables.BasketAnalysisInputValue;

public class BasketAnalysisFilterMapreduceTest {

	private MapReduceDriver<LongWritable, BasketAnalysisInputValue, BasketAnalysisFilterdIntermediateKey, BasketAnalysisFilterdIntermediateValue, NullWritable, NullWritable> driver;

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws IOException {
		Configuration configuration = new Configuration();

		BasketAnalysis basketAnalysis = new BasketAnalysis();
		Job job = new Job(configuration);

		driver = MapReduceDriverFactory.createMapReduceDriver(basketAnalysis
				.setupBeforeFilterMapreduce(job));
	}

	@Test
	public void testMapLongWritableBasketAnalysisInputValueContext() throws IOException {
		driver.addInput(null);
		
		List<Pair<NullWritable,NullWritable>> actual = driver.run();
		
		for (Pair<NullWritable, NullWritable> pair : actual) {
			System.out.println(pair);
		}
	}
}
