package com.mabrlejenka.basketanalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.mabrlejenka.basketanalysis.mapreduce.BasketAnalysisCountingMapper;
import com.mabrlejenka.basketanalysis.mapreduce.BasketAnalysisCountingReducer;
import com.mabrlejenka.basketanalysis.writables.BasketAnalysisFilterdIntermediateKey;
import com.mabrlejenka.basketanalysis.writables.BasketAnalysisFilterdIntermediateValue;

/**
 * <pre>
 * Tool for Basktet Analysis.
 * 
 * </pre>
 * 
 * @author marblejenka
 *
 */
public class BasketAnalysis extends Configured  implements Tool {
	
	private Path source = new Path("input/");
	private Path countedIntermediateSink = new Path("output/counted/");

	@Override
	public int run(String[] args) throws Exception {
		Configuration configuration = getConf();
		
		// delete output path file
		
		for (Job job : jobChain(configuration)) {
			boolean succeeded = job.waitForCompletion(true);
			if (!succeeded) {
				throw new RuntimeException(job.getJobName() + " is failed.");
			}
		}
		
		// delete intermediate file paths
		
		return 0;
	}
	
	public Job[] jobChain(Configuration configuration) throws IOException {
		// setup counting job
		Job counting = new Job(configuration, "Counting job");
		counting.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(counting, source);
		setupCountingMapreduce(counting);
		counting.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(counting, countedIntermediateSink);
		
		// setup hogehoge job
		
		// setup ranking job
		
		return new Job[] { counting };
	}

	public Job setupCountingMapreduce(Job job) throws IOException {
		job.setMapperClass(BasketAnalysisCountingMapper.class);
		job.setMapOutputKeyClass(BasketAnalysisFilterdIntermediateKey.class);
		job.setMapOutputValueClass(BasketAnalysisFilterdIntermediateValue.class);
		
//		job.setPartitionerClass(GenericSecondarySortPartitioner.class);
//		job.setSortComparatorClass(SiteWordRankingIntermediateKeySortComparator.class);
//		job.setGroupingComparatorClass(SiteWordRankingIntermediateKeyGroupingComparator.class);

		job.setReducerClass(BasketAnalysisCountingReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		return job;
	}

	public Job setupCountMapreduce(Job job) throws IOException {
		return null;
	}
	
	public Job setupRankingMapreduce(Job job) throws IOException {
		return null;
	}
}
