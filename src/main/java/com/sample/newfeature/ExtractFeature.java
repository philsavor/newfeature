package com.sample.newfeature;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ExtractFeature extends UDF {
	private Text result = new Text();

	//cls
	private String cls;
	//stime
	private Feature fStime0To6 = new Feature(1);
	private Feature fStime6To9 = new Feature(2);
	private Feature fStime9To12 = new Feature(3);
	private Feature fStime12To15 = new Feature(4);
	private Feature fStime15To18 = new Feature(5);
	private Feature fStime18To23 = new Feature(6);
	private Feature fStimeOther = new Feature(7);
	//p1
	private Feature fP1IPhone = new Feature(8);
	private Feature fP1Android = new Feature(9);
	//net_work
	private Feature fNetwork4g = new Feature(10);
	private Feature fNetwork3g = new Feature(11);
	private Feature fNetwork2g = new Feature(12);
	private Feature fNetworkWifi = new Feature(13);
	private Feature fNetworkOther = new Feature(14);
	//ndays
	private Feature fNDays0 = new Feature(15);
	private Feature fNDays1To2 = new Feature(16);
	private Feature fNDays3To4 = new Feature(17);
	private Feature fNDays5More = new Feature(18);
	//tm
	private Feature fTM20Less = new Feature(19);
	private Feature fTM20To40 = new Feature(20);
	private Feature fTM40More = new Feature(21);
	//pic_num
	private Feature fPicNum1More = new Feature(22);
	//is_video
	private Feature fIsVideo = new Feature(23);
	//text_length
	private Feature fTextLengthLong = new Feature(24);
	//pic_num,is_video,text_length,video_duration
	private Feature fQuality = new Feature(25);

	public Text evaluate( Text action
						,Text stime
						,Text p1
						,Text ua_model
						,Text net_work
						,Integer ndays
						,Integer tm
						,Text publishtime
						,Integer pic_num
						,Text is_video
						,Long	text_length
						,Long	video_duration
						,Map<String,Integer> feed_action) {

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

		try{
			/*
			 * fStime0To6
			 * fStime6To9
			 * fStime9To12
			 * fStime12To15
			 * fStime15To18
			 * fStime18To23
			 * fStimeOther
			 */
			if(null != stime){
				try{
					Long timestamp = Long.parseLong(stime.toString());
					Calendar calendar = Calendar.getInstance();
					calendar.setTimeInMillis(timestamp);
					int hour = Calendar.HOUR;
					switch(hour){
						case 0: case 1: case 2: case 3: case 4: case 5: case 6:
							fStime0To6.setValue("1");
							fStime6To9.setValue("0");
							fStime9To12.setValue("0");
							fStime12To15.setValue("0");
							fStime15To18.setValue("0");
							fStime18To23.setValue("0");
							fStimeOther.setValue("0");
							break;
						case 7: case 8: case 9: 
							fStime0To6.setValue("0");
							fStime6To9.setValue("1");
							fStime9To12.setValue("0");
							fStime12To15.setValue("0");
							fStime15To18.setValue("0");
							fStime18To23.setValue("0");
							fStimeOther.setValue("0");
							break;
						case 10: case 11: case 12: 
							fStime0To6.setValue("0");
							fStime6To9.setValue("0");
							fStime9To12.setValue("1");
							fStime12To15.setValue("0");
							fStime15To18.setValue("0");
							fStime18To23.setValue("0");
							fStimeOther.setValue("0");
							break;
						case 13: case 14: case 15: 
							fStime0To6.setValue("0");
							fStime6To9.setValue("0");
							fStime9To12.setValue("0");
							fStime12To15.setValue("1");
							fStime15To18.setValue("0");
							fStime18To23.setValue("0");
							fStimeOther.setValue("0");
							break;
						case 16: case 17: case 18: 
							fStime0To6.setValue("0");
							fStime6To9.setValue("0");
							fStime9To12.setValue("0");
							fStime12To15.setValue("0");
							fStime15To18.setValue("1");
							fStime18To23.setValue("0");
							fStimeOther.setValue("0");
							break;
						case 19: case 20: case 21: case 22: case 23:
							fStime0To6.setValue("0");
							fStime6To9.setValue("0");
							fStime9To12.setValue("0");
							fStime12To15.setValue("0");
							fStime15To18.setValue("0");
							fStime18To23.setValue("1");
							fStimeOther.setValue("0");
							break;
						default:
							fStime0To6.setValue("0");
							fStime6To9.setValue("0");
							fStime9To12.setValue("0");
							fStime12To15.setValue("0");
							fStime15To18.setValue("0");
							fStime18To23.setValue("0");
							fStimeOther.setValue("1");
							break;
					}
				}catch(Exception e){
					fStime0To6.setValue("0");
					fStime6To9.setValue("0");
					fStime9To12.setValue("0");
					fStime12To15.setValue("0");
					fStime15To18.setValue("0");
					fStime18To23.setValue("0");
					fStimeOther.setValue("1");
				}
			}else{
				fStime0To6.setValue("0");
				fStime6To9.setValue("0");
				fStime9To12.setValue("0");
				fStime12To15.setValue("0");
				fStime15To18.setValue("0");
				fStime18To23.setValue("0");
				fStimeOther.setValue("1");
			}

			/*
			 * fP1IPhone
			 * fP1Android
			 */
			if(null != p1){
				String p1String = p1.toString();
				if(ExtractFeatureConstant.DATA_P1_IPHONE.equals(p1String))
					fP1IPhone.setValue("1");
				else
					fP1IPhone.setValue("0");

				if(ExtractFeatureConstant.DATA_P1_ANDROID.equals(p1String))
					fP1Android.setValue("1");
				else
					fP1Android.setValue("0");
			}else{
				fP1IPhone.setValue("0");
				fP1Android.setValue("0");
			}

			/*
			 * fNetwork4g
			 * fNetwork3g
			 * fNetwork2g
			 * fNetworkWifi
			 * fNetworkOther
			 */
			if(null != net_work){
				String netWorkString = net_work.toString();
				switch(netWorkString){
					case ExtractFeatureConstant.DATA_NETWORK_LTE:
						fNetwork4g.setValue("1");
						fNetwork3g.setValue("0");
						fNetwork2g.setValue("0");
						fNetworkWifi.setValue("0");
						fNetworkOther.setValue("0");
						break;
					case ExtractFeatureConstant.DATA_NETWORK_UMTS:
					case ExtractFeatureConstant.DATA_NETWORK_HSDPA:
					case ExtractFeatureConstant.DATA_NETWORK_HSUPA:
					case ExtractFeatureConstant.DATA_NETWORK_HSPA:
					case ExtractFeatureConstant.DATA_NETWORK_CDMA:
					case ExtractFeatureConstant.DATA_NETWORK_EVDO_0:
					case ExtractFeatureConstant.DATA_NETWORK_EVDO_A:
					case ExtractFeatureConstant.DATA_NETWORK_HSPAP:
						fNetwork4g.setValue("0");
						fNetwork3g.setValue("1");
						fNetwork2g.setValue("0");
						fNetworkWifi.setValue("0");
						fNetworkOther.setValue("0");
						break;
					case ExtractFeatureConstant.DATA_NETWORK_GPRS:
					case ExtractFeatureConstant.DATA_NETWORK_EDGE:
					case ExtractFeatureConstant.DATA_NETWORK_1XRTT:
						fNetwork4g.setValue("0");
						fNetwork3g.setValue("0");
						fNetwork2g.setValue("1");
						fNetworkWifi.setValue("0");
						fNetworkOther.setValue("0");
						break;
					case ExtractFeatureConstant.DATA_NETWORK_WIFI:
					case ExtractFeatureConstant.DATA_NETWORK_ETHERNET:
						fNetwork4g.setValue("0");
						fNetwork3g.setValue("0");
						fNetwork2g.setValue("0");
						fNetworkWifi.setValue("1");
						fNetworkOther.setValue("0");
						break;
					case ExtractFeatureConstant.DATA_NETWORK_NONETWORK_ANDROID:
					case ExtractFeatureConstant.DATA_NETWORK_NONETWORK:
						fNetwork4g.setValue("0");
						fNetwork3g.setValue("0");
						fNetwork2g.setValue("0");
						fNetworkWifi.setValue("0");
						fNetworkOther.setValue("1");
						break;
					default:
						fNetwork4g.setValue("0");
						fNetwork3g.setValue("0");
						fNetwork2g.setValue("0");
						fNetworkWifi.setValue("0");
						fNetworkOther.setValue("1");
						break;
				}
			}else{
				fNetwork4g.setValue("0");
				fNetwork3g.setValue("0");
				fNetwork2g.setValue("0");
				fNetworkWifi.setValue("0");
				fNetworkOther.setValue("1");
			}
			
			/*
			 * fNDays0
			 * fNDays1To2
			 * fNDays3To4
			 * fNDays5More
			 */
			if(null != ndays){
				switch(ndays){
					case 0:
						fNDays0.setValue("1");
						fNDays1To2.setValue("0");
						fNDays3To4.setValue("0");
						fNDays5More.setValue("0");
						break;
					case 1: case 2:
						fNDays0.setValue("0");
						fNDays1To2.setValue("1");
						fNDays3To4.setValue("0");
						fNDays5More.setValue("0");
						break;
					case 3: case 4:
						fNDays0.setValue("0");
						fNDays1To2.setValue("0");
						fNDays3To4.setValue("1");
						fNDays5More.setValue("0");
						break;
					case 5: case 6: case 7:
						fNDays0.setValue("0");
						fNDays1To2.setValue("0");
						fNDays3To4.setValue("0");
						fNDays5More.setValue("1");
						break;
					default:
						fNDays0.setValue("0");
						fNDays1To2.setValue("0");
						fNDays3To4.setValue("0");
						fNDays5More.setValue("0");
						break;
				}
			}else{
				fNDays0.setValue("0");
				fNDays1To2.setValue("0");
				fNDays3To4.setValue("0");
				fNDays5More.setValue("0");	
			}
			
			/*
			 * fTM20Less
			 * fTM20To40
			 * fTM40More
			 */
			if(null != tm){
				double mins = tm / (60 * 7);
				if(mins < 20){
					fTM20Less.setValue("1");
					fTM20To40.setValue("0");
					fTM40More.setValue("0");
				}else if(mins >= 20 && mins <= 40){
					fTM20Less.setValue("0");
					fTM20To40.setValue("1");
					fTM40More.setValue("0");
				}else{
					fTM20Less.setValue("0");
					fTM20To40.setValue("0");
					fTM40More.setValue("1");
				}
			}else{
				fTM20Less.setValue("1");
				fTM20To40.setValue("0");
				fTM40More.setValue("0");
			}
			
			/*
			 * fPicNum1More
			 */
			if(pic_num != null){
				if(pic_num > 0)
					fPicNum1More.setValue("1");
				else
					fPicNum1More.setValue("0");
			}else
				fPicNum1More.setValue("0");
			
			/*
			 * fIsVideo
			 */
			if(is_video != null){
				String isVideoString = is_video.toString();
				switch(isVideoString){
					case ExtractFeatureConstant.DATA_ISVIDEO_TRUE:
						fIsVideo.setValue("1");
						break;
					case ExtractFeatureConstant.DATA_ISVIDEO_FALSE:
						fIsVideo.setValue("0");
						break;
					default:
						fIsVideo.setValue("0");
						break;
				}
			}else
				fIsVideo.setValue("0");
				
			/*
			 * fTextLengthLong
			 */
			if(text_length != null){
				if(text_length > 1000)
					fTextLengthLong.setValue("1");
				else
					fTextLengthLong.setValue("0");
			}else
				fTextLengthLong.setValue("0");
			
			/*
			 * fQuality start
			 */
			Long score = 0L;
			//1.pic_num
			if(null != pic_num && pic_num > 0){
				if(pic_num > 0 && pic_num <= 4)
					score += 10;
				else
					score += 15;
			}
			//2.is_video
			if(null != is_video){
				String isVideoString = is_video.toString();
				switch(isVideoString){
					case ExtractFeatureConstant.DATA_ISVIDEO_TRUE:
						score += 2;
						break;
					default:
						break;
				}
			}
			//3.video_duration
			if(null != video_duration){
				try{
					Long duration = Long.parseLong(video_duration.toString());
					if(duration >= 0 && duration < 3)
						score += 5;
					else if(duration >= 3 && duration < 10)
						score += 2;
				}catch(Exception e){
					//TODO
				}
			}
			//4.feed_action
			if(null != feed_action){
				Integer commentCount = (Integer)feed_action.get("commentCount");
				Integer displayPlayCount =(Integer)feed_action.get("displayPlayCount");
				Integer likeCount = (Integer)feed_action.get("likeCount");
				Integer displayViewCount = (Integer)feed_action.get("displayViewCount");
				Integer shareCount = (Integer)feed_action.get("shareCount");
				
				Integer positiveCount = commentCount + displayPlayCount + likeCount + displayViewCount + shareCount;
				if(positiveCount > 0 && positiveCount <= 5)
					score += 2;
				else if (positiveCount >= 6)
					score += 5;
			}
			//5.publishtime
			if(null != publishtime){
				Long pulishTimeStamp = Long.parseLong(publishtime.toString());
				Long currentTimeStamp = new Date().getTime();
				Long diff = currentTimeStamp - pulishTimeStamp;
				long diffHours = diff / (60 * 60 * 1000);
				double newScore = 0;
				if(diffHours > 0){
					newScore = score * (1 / Math.pow(2,Math.floor(diffHours*1.0/12)));
					if(newScore < 0.1)
						newScore = 0.1;
				}
				if(newScore > 0)
					score = Math.round(newScore);
			}
			
			fQuality.setValue(score.toString());
			/*
			 * fQuality end
			 */
		}catch(Exception e){
			//TODO
		}
		
		
		/*
		 *	setup features' order
		 */
		List<Feature> featureList = new ArrayList<Feature>();
		featureList.add(fStime0To6);
		featureList.add(fStime6To9);
		featureList.add(fStime9To12);
		featureList.add(fStime12To15);
		featureList.add(fStime15To18);
		featureList.add(fStime18To23);
		featureList.add(fStimeOther);
		featureList.add(fP1IPhone);
		featureList.add(fP1Android);
		featureList.add(fNetwork4g);
		featureList.add(fNetwork3g);
		featureList.add(fNetwork2g);
		featureList.add(fNetworkWifi);
		featureList.add(fNetworkOther);
		featureList.add(fNDays0);
		featureList.add(fNDays1To2);
		featureList.add(fNDays3To4);
		featureList.add(fNDays5More);
		featureList.add(fTM20Less);
		featureList.add(fTM20To40);
		featureList.add(fTM40More);
		featureList.add(fPicNum1More);
		featureList.add(fIsVideo);
		featureList.add(fTextLengthLong);
		featureList.add(fQuality);
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
}