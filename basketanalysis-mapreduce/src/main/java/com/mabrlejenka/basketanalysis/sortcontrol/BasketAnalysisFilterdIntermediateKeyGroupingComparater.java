package com.mabrlejenka.basketanalysis.sortcontrol;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class BasketAnalysisFilterdIntermediateKeyGroupingComparater extends WritableComparator {

	protected BasketAnalysisFilterdIntermediateKeyGroupingComparater(
			@SuppressWarnings("rawtypes") Class<? extends WritableComparable> keyClass, boolean createInstances) {
		super(keyClass, createInstances);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
//		if (a instanceof IdentifiedWritableComparable<?> && b instanceof IdentifiedWritableComparable<?>) {
//			Comparable one = IdentifiedWritableComparable.class.cast(a).idenrifier();
//			Comparable other = IdentifiedWritableComparable.class.cast(b).idenrifier();
//
//			return one.compareTo(other);
//		}
		return super.compare(a, b);
	}
}
