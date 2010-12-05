package com.mabrlejenka.basketanalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

import com.mabrlejenka.basketanalysis.mapreduce.BasketAnalysisCountingMapper;

/**
 * <pre>
 * Tool for Basktet Analysis.
 * 
 * 「キーワード検索から１分以内に閲覧したページ群をすべて抽出する」
 * 
 * </pre>
 * 
 * @author marblejenka
 *
 */
public class BasketAnalysis extends Configured  implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public Job[] jobChain(Configuration configuration) throws IOException {
		return null;
	}

	public Job setupBeforeFilterMapreduce(Job job) throws IOException {
		job.setMapperClass(BasketAnalysisCountingMapper.class);
		
		return null;
	}
}
