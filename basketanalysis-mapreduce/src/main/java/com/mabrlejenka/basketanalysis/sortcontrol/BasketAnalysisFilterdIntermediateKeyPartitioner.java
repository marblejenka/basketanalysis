package com.mabrlejenka.basketanalysis.sortcontrol;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.mabrlejenka.basketanalysis.writables.BasketAnalysisFilterdIntermediateKey;

public class BasketAnalysisFilterdIntermediateKeyPartitioner extends
		Partitioner<BasketAnalysisFilterdIntermediateKey, Writable> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getPartition(BasketAnalysisFilterdIntermediateKey key, Writable value, int numPartitions) {
		return Math.abs(key.getUser().hashCode()) % numPartitions;
	}
}
