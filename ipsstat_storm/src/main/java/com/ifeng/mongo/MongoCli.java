package com.ifeng.mongo;

import com.mongodb.MapReduceCommand;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.MapReduceAction;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.*;
import java.util.Map.Entry;

public class MongoCli implements IMongo {

	private MongoClient mongoClient;
	private MongoDatabase db;
	private MongoCollection collection;
	private List<ServerAddress> serverAddresses;
	private List<MongoCredential> credentials;

	public MongoCli(List<ServerAddress> serverAddresses , List<MongoCredential> credentials) {
	    this.serverAddresses=serverAddresses;
	    this.credentials =credentials;
		initMongo();
	}
	private void initMongo() {
		try {
			mongoClient = new MongoClient(serverAddresses, credentials);
		} catch (Exception e) {

		}
	}

	/**
	 * 切换数据库
	 * 
	 * @param dbname
	 *            数据库名称
	 */
	public void changeDb(String dbname) {
		if (mongoClient == null) {
			throw new NullPointerException();
		}

		db = mongoClient.getDatabase(dbname);
	}

	/**
	 * 获取集合
	 *
	 * @param name
	 *            集合名称
	 */
	public void getCollection(String name) {
		if (db == null) {
			throw new NullPointerException();
		}
		collection = db.getCollection(name);
	}

	public MongoCollection getCollections(String name) {
		if (db == null) {
			throw new NullPointerException();
		}
		return  db.getCollection(name);
	}


	private Document createUpdateFields(Map<String, Object> fields) {
		Document result = new Document();
		if (fields != null) {
			for (Entry<String, Object> item : fields.entrySet()) {
				result.put(item.getKey(), item.getValue());
			}
		}
		return new Document("$set",result);
	}

	private Map<String, String> createMapReduce(MongoSelect select) throws Exception {
		if (select.getGroupBy() != null && select.getGroupBy().size() > 0) {
			StringBuilder values = new StringBuilder();
			StringBuilder recudeVar = new StringBuilder();
			List<String> vars = new ArrayList<String>();
			boolean hasAvgMethod = false;
			for (SelectField item : select.getFields()) {
				String fn = item.getAlias().trim();
				String name = item.getName().trim().toLowerCase();
				if (name.contains("sum(")) {
					values.append(fn).append(":this.").append(item.getName().replace("sum(", "").replace(")", "")).append(",");
					recudeVar.append("var ").append(fn).append("=0;");
					if (!vars.contains(fn)) {
						vars.add(fn);
					} else {
						throw new Exception("查询字段名不能重复.");
					}
				}
				if (name.contains("count(")) {
					values.append(fn).append(":1,");
					recudeVar.append("var ").append(fn).append("=0;");
					if (!vars.contains(fn)) {
						vars.add(fn);
					} else {
						throw new Exception("查询字段名不能重复.");
					}
				}
				if (name.contains("max(")) {
					values.append(fn).append(":Math.max(this.").append(item.getName().replace("max(", "")).append(",");
					recudeVar.append("var ").append(fn).append("=0;");
					if (!vars.contains(fn)) {
						vars.add(fn);
					} else {
						throw new Exception("查询字段名不能重复.");
					}
				}
				if (name.contains("min(")) {
					values.append(fn).append(":Math.min(this.").append(item.getName().replace("min(", "")).append(",");
					recudeVar.append("var ").append(fn).append("=0;");
					if (!vars.contains(fn)) {
						vars.add(fn);
					} else {
						throw new Exception("查询字段名不能重复.");
					}
				}
				if (name.contains("avg(")) {
					hasAvgMethod = true;
					values.append(fn).append(":this.").append(item.getName().replace("avg(", "").replace(")", "")).append(",");
					recudeVar.append("var ").append(fn).append("=0;");
					if (!vars.contains(fn)) {
						vars.add(fn);
					} else {
						throw new Exception("查询字段名不能重复.");
					}
				}
			}
			if (values.length() <= 0) {
				return null;
			}
			values = values.deleteCharAt(values.length() - 1);
			StringBuilder groupKeys = new StringBuilder();
			for (String item : select.getGroupBy()) {
				groupKeys.append(item).append(":").append("this.").append(item).append(",");
			}
			groupKeys = groupKeys.deleteCharAt(groupKeys.length() - 1);

			StringBuilder mapCode = new StringBuilder();
			mapCode.append("function(){emit({").append(groupKeys).append("},{").append(values).append("});}");

			StringBuilder finalizeFunc = new StringBuilder();
			StringBuilder reduceCode = new StringBuilder();
			StringBuilder returnObj = new StringBuilder("return {");

			reduceCode.append("function(key,values){");
			reduceCode.append(recudeVar);
			reduceCode.append("for(var i = 0; i < values.length;i++){");

			for (String var : vars) {
				reduceCode.append(var).append(" += values[i].").append(var).append(";");
				returnObj.append(var).append(":").append(var).append(",");
			}
			if (hasAvgMethod) {
				finalizeFunc.append("function (key, reducedValue) {").append("reducedValue.Avg = reducedValue.Sum/reducedValue.Count;")
						.append("return reducedValue;}");
			}

			returnObj = returnObj.length() > 8 ? returnObj.deleteCharAt(returnObj.length() - 1).append("}") : new StringBuilder("");
			reduceCode.append("} ").append(returnObj).append(";}");
			Map<String, String> result = new HashMap<String, String>();
			result.put("map", mapCode.toString());
			result.put("reduce", reduceCode.toString());
			return result;
		}
		return null;
	}

	@Override
	public <T extends EntyCodec> List<T> distinct(MongoSelect select, Class<T> classType){
//		Document fields = select.createFieldsDocument();
//		Document where = select.getCondition().toDocument();
//		Document sort = select.createSortDocument();
//
//		MongoCursor cursor = collectionfilter(where).skip(select.getPageIndex()).limit(select.getPageSize()).batchSize(select.getPageSize()).sort(sort)
//				.iterator();
		return null;
	}
	@Override
	public <T extends EntyCodec> List<T> selectAll(MongoSelect select, Class<T> classType) throws Exception {
		if (collection == null || select == null){
			throw new NullPointerException();
		}

		Document sort = select.createSortDocument();
		Document fields = select.createFieldsDocument();
		MongoCursor cursor = null;
		boolean isMapReduce = false;
		if (select.getGroupBy() != null && select.getGroupBy().size() > 0) {
			Map<String, String> mapReduce = createMapReduce(select);
			MapReduceIterable output = collection.mapReduce(mapReduce.get("map"), mapReduce.get("reduce"));
			cursor = output.action(MapReduceAction.REPLACE).collectionName("reduceCollection")
					.limit(select.getPageIndex()).batchSize(select.getPageSize()).sort(sort).iterator();
			isMapReduce = true;
		} else {
			cursor =collection.find(fields).skip(select.getPageIndex()).limit(select.getPageSize()).batchSize(select.getPageSize()).sort(sort).iterator();
		}

		ArrayList<T> list = new ArrayList<T>();
		T en = null;
		while (cursor.hasNext()) {
			en = classType.newInstance();
			DataLoader loader = new DataLoader((Document)cursor.next(),isMapReduce);
			en.decode(loader);
			list.add(en);
		}

		return list;
	}

	@Override
	public <T extends EntyCodec> T selectOne(MongoSelect select, Class<T> classType) throws Exception {
		if (collection == null || select == null){
			throw new NullPointerException();
		}
		T en = classType.newInstance();
		Document where = select.getCondition().toDocument();
		Document fields = select.createFieldsDocument();
		Document sort = select.createSortDocument();

		FindIterable res = collection.find(fields).filter(where).sort(sort);
		if (res == null || !res.iterator().hasNext())
			return null;

		DataLoader loader = new DataLoader((Document)res.iterator().next());
		en.decode(loader);

		return en;
	}

	@Override
	public <T extends EntyCodec> List<T> selectList(MongoSelect select, Class<T> classType) throws Exception {
		if (collection == null){
			throw new NullPointerException();
		}
		Document fields =  select.createFieldsDocument();
		Document where = select.getCondition().toDocument();
		Document sort = select.createSortDocument();

		boolean isMapReduce = false;

		MongoCursor cursor = null;
		if (select != null) {

			if (select.getGroupBy() != null && select.getGroupBy().size() > 0) {
				Map<String, String> mapReduce = createMapReduce(select);
				MapReduceIterable output = collection.mapReduce(mapReduce.get("map"), mapReduce.get("reduce"));
				cursor = output.filter(where).action(MapReduceAction.REPLACE).collectionName("reduceCollection")
						.limit(select.getPageIndex()).batchSize(select.getPageSize()).sort(sort).iterator();

				isMapReduce = true;
			} else {
				cursor = collection.find(fields).filter(where).skip(select.getPageIndex()).limit(select.getPageSize()).batchSize(select.getPageSize()).sort(sort)
				.iterator();
			}
		}

		ArrayList<T> list = new ArrayList<T>();
		T en = null;
		while (cursor.hasNext()) {
			en = classType.newInstance();
			DataLoader loader = new DataLoader((Document)cursor.next(), isMapReduce);
			en.decode(loader);
			list.add(en);
		}
		return list;
	}

	@Override
	public DeleteResult remove(Where where) throws Exception {
		if (collection == null){
			throw new NullPointerException();
		}
		Document condition = where == null ? new Document():where.toDocument();
		return collection.deleteMany(condition);
	}

	@Override
	public UpdateResult update(Map<String, Object> fields, Where where) throws Exception {
		if (collection == null){
			throw new NullPointerException();
		}
		Document condition = where == null ? new Document():where.toDocument();
		Document updates = createUpdateFields(fields);
		return collection.updateMany(condition,updates);
	}

	@Override
	public <T extends IEncode> void insert(T en) throws Exception {
		if (collection == null){
			throw new NullPointerException();
		}

		Document document = en.encode();
		collection.insertOne(document, new InsertOneOptions().bypassDocumentValidation(true));
	}

	@Override
	public <T extends IEncode> void insert(List<T> list) throws Exception {
		if (collection == null){
			throw new NullPointerException();
		}
		List<Document> documents = new ArrayList<Document>();
		for (T t : list) {
			documents.add(t.encode());
		}
		collection.insertMany(documents);
	}


	@Override
	public <T extends IEncode> void insert(T en, Date expire) throws Exception {
		if (collection == null){
			throw new NullPointerException();
		}

		Document document = en.encode();
		expire = expire == null ? expire : new Date();
		document.put("delkey",expire);
		collection.insertOne(document, new InsertOneOptions().bypassDocumentValidation(true));
	}

	@Override
	public <T extends IEncode> void insert(List<T> list, Date expire) throws Exception {
		if (collection == null){
			throw new NullPointerException();
		}
		List<Document> documents = new ArrayList<Document>();

		Document doc = null;
		for (T t : list) {
			doc = t.encode();
			doc.put("delkey",expire);
			documents.add(doc);
		}
		collection.insertMany(documents);
	}

	@Override
	public MongoCursor mapReduce(String map, String reduce, String outputTarget,
								 MapReduceCommand.OutputType outputType, Where where) throws Exception{
		if (collection == null){
			throw new NullPointerException();
		}
		Document cond = where == null ? new Document():where.toDocument();
		MapReduceIterable mapReduceIterable = collection.mapReduce(map, reduce);
		MongoCursor cusor = mapReduceIterable.action(MapReduceAction.REPLACE).collectionName(outputTarget)
				.filter(cond).iterator();
		return cusor;
	}

	@Override
	public void close() throws Exception{
		if (mongoClient == null){
			throw new NullPointerException();
		}
		try {
			mongoClient.close();
		} catch (Exception e) {
		}
		finally{
			collection = null;
			mongoClient =null;
			db = null;
		}
	}

	// ===========Setter Getter======================
	public List<MongoCredential> getCredentials() {
		return credentials;
	}

	public void setCredentials(List<MongoCredential> credentials) {
		this.credentials = credentials;
	}

	public List<ServerAddress> getServerAddresses() {
		return serverAddresses;
	}

	public void setServerAddresses(List<ServerAddress> serverAddresses) {
		this.serverAddresses = serverAddresses;
	}
}
