package com.bonc.storm.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.trident.Stream;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class Parse {
	public static Stream exec(Stream stream) {
		Stream stream1 = stream.each(new Fields("str"), new MyParse(), new Fields("list"));
		return stream1;
	}

	public static class MyParse extends BaseFunction {
		private static final long serialVersionUID = 1L;
		public void execute(TridentTuple tuple, TridentCollector collector) {
			//页面配置的字段下标为：0,1,2,4,5
			//对应的字段名称为：name,age,sex,address,phone
			//String[] values = StringUtils.splitPreserveAllTokens(tuple.getString(0), "^");//用分隔符切割
			String[] values = tuple.getString(0).split("^");
			int[] index = new int[]{0,1,2};//页面传过来的下标
			List<String> list = new ArrayList<String>();
			for (int i : index) {
				list.add(values[i]);
			}
			collector.emit(new Values(list));
		}
	}
}
