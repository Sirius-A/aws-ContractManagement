package ch.hsr.bll;

import java.util.ArrayList;
import java.util.Comparator;

import ch.hsr.dal.CDAR_DatabaseMongoDB;

public class CDAR_Customer implements Comparator<CDAR_Customer> {

	private String id;
	private String name;
	private String location;
	private String zip;

	static CDAR_DatabaseMongoDB db = new CDAR_DatabaseMongoDB();

	public CDAR_Customer(String id, String name, String location, String zip) {
		initFromDb(id, name, location, zip);
	}

	public CDAR_Customer(String name, String location, String zip) {
		addToDb(name, location, zip);
	}
	
	private void initFromDb(String id, String name, String location, String zip) {
		this.id = id;
		this.name = name;
		this.location = location;
		this.zip = zip;
	}

	private void addToDb(String name, String location, String zip) {
		this.name = name;
		this.location = location;
		this.zip = zip;
		db.addEntry(this);
	}

	public String getName() {
		return name;
	}

	public String getID() {
		return id;
	}
	
	public String getLocation() {
		return location;
	}
	
	public String getZip() {
		return zip;
	}

	public static ArrayList<CDAR_Customer> getEntries() throws Exception {
		ArrayList<CDAR_Customer> customer = db.getCustomers();
		return customer;
	}

	@Override
	public int compare(CDAR_Customer o1, CDAR_Customer o2) {		
			return Integer.parseInt(((CDAR_Customer) o1).getID())
					- Integer.parseInt(((CDAR_Customer) o2).getID());
		
	}

}
