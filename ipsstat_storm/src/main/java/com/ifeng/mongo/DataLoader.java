
package com.ifeng.mongo;
import org.bson.Document;

public class DataLoader {
	private Document dbObject;
	
	public DataLoader(Document obj) {
		this.dbObject = obj;
	}
	
	public DataLoader(Document obj,boolean isMapReduceResult) {
		if (isMapReduceResult){
			dbObject = new Document();
			Document _id = (Document)obj.get("_id");
			for (String item : _id.keySet()) {
				dbObject.put(item, _id.get(item));
			}

			Document _values = (Document)obj.get("value");
			for (String item : _values.keySet()) {
				dbObject.put(item, _values.get(item));
			}
		}
		else {
			this.dbObject = obj;
		}
	}
	
	public int getInt(String key,int defaultValue) {
		if (dbObject.containsKey(key)){
			return (int)Double.parseDouble(dbObject.get(key).toString());
		}
		return defaultValue;
	}
	
	public int getInt(String key) {
		return getInt(key,0);
	}
	
	public String getString(String key) {
		if (dbObject.containsKey(key)){
			return dbObject.get(key).toString();
		}
		return "";
	}
	
	public DataLoader getLoader(String key) throws Exception {
		try {
			if (dbObject.containsKey(key)){
				return new DataLoader((Document)dbObject.get(key));
			}
		} catch (Exception e) {
			throw e;
		}
		return null;
	}
}
