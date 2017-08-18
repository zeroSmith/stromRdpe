package com.bonc.rdpe.storm.real;

import org.apache.storm.trident.Stream;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class ParseStream {

	public static Stream exec(Stream stream) {
		Stream stream1 = stream.each(new Fields(""), new Parse(),
				new Fields(""));
		return stream1;
	}

	public static class Parse extends BaseFunction {
		private static final long serialVersionUID = 1L;

		public void execute(TridentTuple tuple, TridentCollector collector) {
			String[] colvalues = tuple.getString(0).split(" ");
			String[] arrRet = new String[colvalues.length];// 用来存放需要向下传递的数据。
			String[] arrIndexs = new String[] { "1", "3" };// 前台传入下标
			for (int i = 0; i < arrIndexs.length; i++) {
				arrRet[Integer.parseInt(arrIndexs[i])] = colvalues[Integer.parseInt(arrIndexs[i])];
			}
			collector.emit(new Values(arrRet));
		}
	}
}