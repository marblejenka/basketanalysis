package com.mabrlejenka.basketanalysis.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriverFactory;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.mabrlejenka.basketanalysis.BasketAnalysis;
import com.mabrlejenka.basketanalysis.utils.Constants;
import com.mabrlejenka.basketanalysis.writables.BasketAnalysisFilterdIntermediateKey;
import com.mabrlejenka.basketanalysis.writables.BasketAnalysisFilterdIntermediateValue;

public class BasketAnalysisFilterMapreduceTest {

	private MapReduceDriver<LongWritable, Text, BasketAnalysisFilterdIntermediateKey, BasketAnalysisFilterdIntermediateValue, LongWritable, Text> driver;

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws IOException {
		Configuration configuration = new Configuration();

		BasketAnalysis basketAnalysis = new BasketAnalysis();
		Job job = new Job(configuration);

		driver = MapReduceDriverFactory.createMapReduceDriver(basketAnalysis
				.setupCountingMapreduce(job));
	}

	@Test
	public void testMapLongWritableBasketAnalysisInputValueContext()
			throws IOException {
		driver.addInput(buildInput(1, "A", 970916004116L, "lake counrty, ca"));

		List<Pair<LongWritable, Text>> actual = driver.run();

		for (Pair<LongWritable, Text> pair : actual) {
			System.out.println(pair);
		}
	}

	public Pair<LongWritable, Text> buildInput(long key, String user,
			long unixDateTime, String words) {
		return new Pair<LongWritable, Text>(new LongWritable(key), new Text(
				user + Constants.TEXT_DELIMITER + unixDateTime
						+ Constants.TEXT_DELIMITER + words));
	}
}
