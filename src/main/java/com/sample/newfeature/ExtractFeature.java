package com.sample.newfeature;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ExtractFeature extends UDF {
	private Text result = new Text();

	//cls
	private String cls;
	//stime
	private Feature fCs = new Feature(1);
	private Feature fCsFreshness = new Feature(2);
	private Feature fCsDisplayViewCount = new Feature(3);
	private Feature fCsDisplayPlayCount = new Feature(4);
	private Feature fCsAgreeCount = new Feature(5);
	private Feature fCsCommentCount = new Feature(6);
	private Feature fCsFwCount = new Feature(7);
	private Feature fCsOriginalViewCount = new Feature(8);
	private Feature fCsOriginalPlayCount = new Feature(9);
	private Feature fCsOriginalLikeCount = new Feature(10);
	private Feature fCsOriginalCommentCount = new Feature(11);
	private Feature fCsOriginalShareCount = new Feature(12);
	private Feature fFreshness = new Feature(13);
	private Feature fDisplayViewCount = new Feature(14);
	private Feature fDisplayPlayCount = new Feature(15);
	private Feature fAgreeCount = new Feature(16);
	private Feature fCommentCount = new Feature(17);
	private Feature fFwCount = new Feature(18);
	private Feature fOriginalViewCount = new Feature(19);
	private Feature fOriginalPlayCount = new Feature(20);
	private Feature fOriginalLikeCount = new Feature(21);
	private Feature fOriginalCommentCount = new Feature(22);
	private Feature fOriginalShareCount = new Feature(23);

	public Text evaluate(Text action
						,Double cs
						,Text publishtime
						,Integer displayviewcount
						,Integer displayplaycount
						,Integer likecount
						,Integer commentcount
						,Integer sharecount
						,Integer org_viewcount
						,Integer org_playcount
						,Integer org_likecount
						,Integer org_commentcount
						,Integer org_sharecount) {

		StringBuilder builder = new StringBuilder();

		/*
		 * cls
		 */
		if(null != action){
			String actionString = action.toString();
			switch(actionString){
				case ExtractFeatureConstant.DATA_ACTION_CONTENT:
				case ExtractFeatureConstant.DATA_ACTION_SHARE:
				case ExtractFeatureConstant.DATA_ACTION_COMMENT:
				case ExtractFeatureConstant.DATA_ACTION_COLLECT:
				case ExtractFeatureConstant.DATA_ACTION_LIKE:
					cls = "1";
					break;
				case ExtractFeatureConstant.DATA_ACTION_NEGATIVE_FEEDBACK:
				case ExtractFeatureConstant.DATA_ACTION_CANCEL_LIKE:
				case ExtractFeatureConstant.DATA_ACTION_REVEAL:
					cls = "0";
					break;
				default:
					cls = "0";
					break;
			}
		}else
			cls = "0";

		/*
		 * fCs
		 */
		//TODO
		fCs.setValue(cs.toString());
		
		/*
		 * fFreshness
		 * fCsFreshness
		 */
		Double freshness;
		if(null != publishtime){
			Long pulishTimeStamp = Long.parseLong(publishtime.toString());
			Long currentTimeStamp = new Date().getTime();
			Long diff = currentTimeStamp - pulishTimeStamp;
			long diffDay = diff / (24 * 60 * 60 * 1000);
			long diffHours = diff / (60 * 60 * 1000);
			long diffMin = diff / (60 * 1000);
			freshness = Math.pow(0.9, diffDay) + Math.pow(0.99, diffHours) + Math.pow(0.9999, diffMin);
			
			//fFreshness
			freshness = sigmoid(freshness);
			fFreshness.setValue(freshness.toString());
			
			//fCsFreshness
			Double csFreshness = freshness * cs;
			csFreshness = sigmoid(csFreshness);
			fCsFreshness.setValue(csFreshness.toString());
		}else{
			fFreshness.setValue("0.0");
			fCsFreshness.setValue("0.0");
		}
		
		/*
		 * fDisplayViewCount,fCsDisplayViewCount
		 * fDisplayPlayCount,fCsOriginalPlayCount
		 * fAgreeCount,fCsAgreeCount
		 * fCommentCount,fCsCommentCount
		 * fFwCount,fCsFwCount
		 * fOriginalViewCount,fCsOriginalViewCount
		 * fOriginalPlayCount,fCsOriginalPlayCount
		 * fOriginalLikeCount,fCsOriginalLikeCount
		 * fOriginalCommentCount,fCsOriginalCommentCount
		 * fOriginalShareCount,fCsOriginalShareCount
		 */
		setUpCount(cs,displayviewcount,fDisplayViewCount,fCsDisplayViewCount);
		setUpCount(cs,displayplaycount,fDisplayPlayCount,fCsOriginalPlayCount);
		setUpCount(cs,likecount,fAgreeCount,fCsAgreeCount);
		setUpCount(cs,commentcount,fCommentCount,fCsCommentCount);
		setUpCount(cs,sharecount,fFwCount,fCsFwCount);
		setUpCount(cs,org_viewcount,fOriginalViewCount,fCsOriginalViewCount);
		setUpCount(cs,org_playcount,fOriginalPlayCount,fCsOriginalPlayCount);
		setUpCount(cs,org_likecount,fOriginalLikeCount,fCsOriginalLikeCount);
		setUpCount(cs,org_commentcount,fOriginalCommentCount,fCsOriginalCommentCount);
		setUpCount(cs,org_sharecount,fOriginalShareCount,fCsOriginalShareCount);
		
		/*
		 *	setup features' order
		 */
		List<Feature> featureList = new ArrayList<Feature>();
		featureList.add(fCs);
		featureList.add(fFreshness);
		featureList.add(fCsFreshness);
		featureList.add(fDisplayViewCount);
		featureList.add(fCsDisplayViewCount);
		featureList.add(fDisplayPlayCount);
		featureList.add(fCsDisplayPlayCount);
		featureList.add(fAgreeCount);
		featureList.add(fCsAgreeCount);
		featureList.add(fCommentCount);
		featureList.add(fCsCommentCount);
		featureList.add(fFwCount);
		featureList.add(fCsFwCount);
		featureList.add(fOriginalViewCount);
		featureList.add(fCsOriginalViewCount);
		featureList.add(fOriginalPlayCount);
		featureList.add(fCsOriginalPlayCount);
		featureList.add(fOriginalLikeCount);
		featureList.add(fCsOriginalLikeCount);
		featureList.add(fOriginalCommentCount);
		featureList.add(fCsOriginalCommentCount);
		featureList.add(fOriginalShareCount);
		featureList.add(fCsOriginalShareCount);
		Collections.sort(featureList);
		
		builder.append(cls + " "); 
		for(Feature f : featureList){
			if(f.getValue() != null)
				builder.append(MessageFormat.format(ExtractFeatureConstant.FEATURE_STRING_FORMAT, f.getIndex(), f.getValue()) + " ");
		}
		/*
		 *	setup features' order
		 */

		result.set(builder.toString().trim());

		return result;
	}
	
	private double sigmoid(double x){
        return 1 / (1 + Math.exp(-x));
    }
	
	private void setUpCount(Double cs, Integer originalCount, Feature feature, Feature csFeature){
		if(null != originalCount){
			Double count = originalCount * 1.0;
			Double f = sigmoid(count);
			feature.setValue(f.toString());
			
			Double csF = f * cs;
			csF = sigmoid(csF);
			csFeature.setValue(csF.toString());
		}else{
			feature.setValue("0");
			csFeature.setValue("0");
		}
	}
}