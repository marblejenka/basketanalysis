package com.mabrlejenka.basketanalysis.sortcontrol;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * セカンダリーソートのための汎用的なキーコンパレーターです
 * 
 * @author shingo.furuyama
 * 
 */
public class BasketAnalysisFilterdIntermediateKeySortComparater extends WritableComparator {

	protected BasketAnalysisFilterdIntermediateKeySortComparater(
			@SuppressWarnings("rawtypes") Class<? extends WritableComparable> keyClass, boolean createInstances) {
		super(keyClass, createInstances);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
//		if (a instanceof IdentifiedWritableComparable<?> && b instanceof IdentifiedWritableComparable<?>) {
//			Comparable oneSortValue = IdentifiedWritableComparable.class.cast(a).idenrifier();
//			Comparable anotherSortValue = IdentifiedWritableComparable.class.cast(b).idenrifier();
//
//			int sortValueCompare = oneSortValue.compareTo(anotherSortValue);
//			if (sortValueCompare != 0) {
//				return sortValueCompare;
//			}
//
//			Comparable oneIdentifier = IdentifiedWritableComparable.class.cast(a).groupKey();
//			Comparable anotherIdentifier = IdentifiedWritableComparable.class.cast(b).groupKey();
//			return oneIdentifier.compareTo(anotherIdentifier);
//		}
		return super.compare(a, b);
	}
}
