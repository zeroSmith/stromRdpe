package com.bonc.storm.test;

import org.apache.storm.trident.Stream;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;

public class FilterStream {
	public static Stream exec(Stream stream) {
		Stream streams = stream.each(new Fields(), new MyFilter()).parallelismHint(2);
				
		return streams;
	}

	public static class MyFilter extends BaseFilter {
		private static final long serialVersionUID = 1L;
		public boolean isKeep(TridentTuple tuple) {
			for(String var:tuple.getString(0).split("^")){
				System.err.println(var );
			}
			return true;
		}

	}

}
