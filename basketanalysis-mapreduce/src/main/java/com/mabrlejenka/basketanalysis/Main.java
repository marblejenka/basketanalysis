package com.mabrlejenka.basketanalysis;

import org.apache.hadoop.util.ToolRunner;

public class Main {
	public static void main(String[] args) {
		System.out.println("START ANALYSIS");
		try {
			ToolRunner.run(new BasketAnalysis(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("END ANALYSIS");
	}
}
