package com.bonc.storm.test;

import org.apache.storm.shade.org.apache.commons.exec.util.StringUtils;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
public class ParseStream {

	public static Stream exec(Stream stream) {
		Stream stream1 = stream.each( new Parse(),new Fields("outPutFields"));
		return stream1;
	}

	public static class Parse extends BaseFunction {
		private static final long serialVersionUID = 1L;
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String[] colvalues = tuple.getString(0).split("^");
			//StringUtils.split(tuple.getString(0), "^");
			String[] arrRet = new String[colvalues.length];// 用来存放需要向下传递的数据。
			String[] arrIndexs = new String[] { "1","2" };// 页面传入下标
			for (int i = 0; i < arrIndexs.length; i++) {
				arrRet[Integer.parseInt(arrIndexs[i])] = colvalues[Integer.parseInt(arrIndexs[i])];
			}
			System.out.println("arrRet.length=   "+arrRet.length);
			//collector.emit(new Values(arrRet));
		}
	}

}
