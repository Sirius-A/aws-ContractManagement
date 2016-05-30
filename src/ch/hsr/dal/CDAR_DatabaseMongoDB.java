package ch.hsr.dal;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

import com.mongodb.*;
import org.bson.BSONObject;
import org.bson.types.ObjectId;

import ch.hsr.bll.CDAR_Contract;
import ch.hsr.bll.CDAR_Customer;
import ch.hsr.bll.CDAR_CustomerContractJoin;

public class CDAR_DatabaseMongoDB {
	private static final String ZIP = "zip";
	private static final String LOCATION = "location";
	private static final String NAME = "name";
	private static final String OID = "_id";
	private static final String DATE = "date";
	private static final String DESCRIPTION = "description";
	private static final String CONTRACT = "contract";
	private static final String CUSTOMER = "customer";
	
	private DB db;
	private MongoClientURI mongoURI;

	public CDAR_DatabaseMongoDB() {
		init();
	}

	private void init() {
		Properties prop = new Properties();
		try {
			InputStream in = getClass().getClassLoader().getResourceAsStream("config.properties");
			prop.load(in);
			mongoURI = new MongoClientURI(prop.getProperty("mongoURI"));
			// db = mongoURI.connectDB();
			// db.authenticate(mongoURI.getUsername(), mongoURI.getPassword());
			clearCollection(CUSTOMER);
			clearCollection(CONTRACT);
		} catch (MongoException | IOException e) {
			e.printStackTrace();
		}
	}
	
	public void addEntry(CDAR_Contract contract) throws Exception {
		try {
			DBCollection collContracts = db.getCollection(CONTRACT);
			BasicDBObject doc = new BasicDBObject(DESCRIPTION, contract.getDescription()).append(CUSTOMER, getCustomer(contract.getRefID().toString())).append(DATE, contract.getDate());
			collContracts.insert(doc);
		} catch (Exception e) {
			throw e;
		}
	}

	public void addEntry(CDAR_Customer customer) {
		try {
			DBCollection coll = db.getCollection(CUSTOMER);
			BasicDBObject doc = new BasicDBObject(NAME, customer.getName()).append(LOCATION,customer.getLocation()).append(ZIP, customer.getZip());
			coll.insert(doc);
			System.out.println(doc.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public ArrayList<CDAR_Contract> getContracts() {
		ArrayList<CDAR_Contract> list = new ArrayList<CDAR_Contract>();

		try {
			DBCollection coll = db.getCollection(CONTRACT);
			DBCursor cursor = coll.find();
			while (cursor.hasNext()) {
				DBObject element = cursor.next();
				String idString = element.get(OID).toString();
				String description = element.get(DESCRIPTION).toString();
				DBObject customer = (DBObject) element.get(CUSTOMER);
				String customerId = customer.get(OID).toString();
				String date = element.get(DATE).toString();
				list.add(new CDAR_Contract(idString, date, description, customerId));
			}
		} catch (MongoException e) {
			e.printStackTrace();
		}
		return list;
	}

	public ArrayList<CDAR_Customer> getCustomers() {
		ArrayList<CDAR_Customer> list = new ArrayList<CDAR_Customer>();

		try {
			DBCollection coll = db.getCollection(CUSTOMER);

			DBCursor cursor = coll.find();
			while (cursor.hasNext()) {
				DBObject element = cursor.next();
				String idString = element.get(OID).toString();
				String name = element.get(NAME).toString();
				String location = element.get(LOCATION).toString();
				String zip = element.get(ZIP).toString();
				list.add(new CDAR_Customer(idString, name, location, zip));
			}
		} catch (MongoException e) {
			e.printStackTrace();
		}
		return list;
	}

	
	public ArrayList<CDAR_CustomerContractJoin> getJoins() {
		ArrayList<CDAR_CustomerContractJoin> list = new ArrayList<CDAR_CustomerContractJoin>();
		try {
			DBCollection collContracts = db.getCollection(CONTRACT);
			collContracts.distinct(DESCRIPTION);
			DBCursor cursorContracts = collContracts.find();
			while (cursorContracts.hasNext()) {
				DBObject contract = cursorContracts.next();
				String contractDescription = contract.get(DESCRIPTION).toString();
				String contractDate = contract.get(DATE).toString();
				DBObject customer = (DBObject) contract.get(CUSTOMER);
				String customer_id = customer.get(OID).toString();
				String customerName = customer.get(NAME).toString();
				String customerLocation = customer.get(LOCATION).toString();
				String customerZip = customer.get(ZIP).toString();
				list.add(new CDAR_CustomerContractJoin(customer_id, customerName, customerLocation, customerZip, contractDate, contractDescription));
			}
		} catch (MongoException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	private DBObject getCustomer(String id) throws Exception {
		DBCollection collCustomers = db.getCollection(CUSTOMER);
		ObjectId _id= new ObjectId(id);
		BasicDBObject obj = new BasicDBObject();
		obj.append(OID, _id);
		BasicDBObject query = new BasicDBObject();
		query.putAll((BSONObject)query);
		DBObject customer = collCustomers.findOne(query);
		if (customer == null) {
			throw new Exception("No customer found");
		}
		return collCustomers.findOne(query);
	}
	
	private void clearCollection(String name) {
		try {
			DBCollection coll = db.getCollection(name);

			DBCursor cursor = coll.find();
			while (cursor.hasNext()) {
				DBObject element = cursor.next();
				coll.remove(element);
			}
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}
}
