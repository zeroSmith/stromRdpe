package com.bonc.rdpe;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class MessageScheme implements Scheme {

    public List<Object> deserialize(byte[] arg0) {
        try{
         String msg = new String(arg0, "UTF-8");
         String msg_0 = "hello";
         return new Values(msg_0,msg);
        }
        catch (UnsupportedEncodingException  e) {
            // TODO: handle exception
            e.printStackTrace();
        }
        return null;
    }

    public Fields getOutputFields() {
        
        return new Fields("key","message");
    }

	public List<Object> deserialize(ByteBuffer arg0) {
		// TODO Auto-generated method stub
		return null;
	}
    
}