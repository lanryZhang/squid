package com.ifeng.mongo;


import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import java.util.ArrayList;
import java.util.List;

public class MongoFactory {

	private static MongoCli instance;

	/**
	 * 默认数据库
	 * @return
	 */
	public static MongoCli getInstance()  {
		if (instance == null){
			synchronized (MongoFactory.class) {
				if (instance == null){
					ServerAddress addr21 = new ServerAddress("10.50.16.21", 27021);
					ServerAddress addr35 = new ServerAddress("10.50.16.35", 27021);
					ServerAddress addr36 = new ServerAddress("10.50.16.36", 27021);
					ServerAddress addr18 = new ServerAddress("10.50.16.18", 27021);
					ServerAddress addr20 = new ServerAddress("10.50.16.20", 27017);
					List<ServerAddress> list = new ArrayList<ServerAddress>();
					list.add(addr20);
					instance = new MongoCli(list,new ArrayList<MongoCredential>());
				}
			}
		}
		return instance;
	}
	
	/**
	 * 初始化Mongo实例，并且切换到指定数据库
	 * @param dbname
	 * @return
	 */
//	public static MongoCli getInstance(String dbname){
//		if (instance == null){
//			synchronized (MongoFactory.class) {
//				if (instance == null){
//					instance = new MongoCli();
//					instance.changeDb(dbname);
//				}
//			}
//		}
//		return instance;
//	}
}
