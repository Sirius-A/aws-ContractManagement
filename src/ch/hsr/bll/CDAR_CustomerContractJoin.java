package ch.hsr.bll;

import java.util.*;

import ch.hsr.dal.CDAR_DatabaseMongoDB;

public class CDAR_CustomerContractJoin {

	private String customer_id;
	private String name;
	private String location;
	private String zip;
	private String date;
	private String description;

	static CDAR_DatabaseMongoDB db = new CDAR_DatabaseMongoDB();

	public CDAR_CustomerContractJoin(String customer_id, String name, String location, String zip, String date, String description) {
		init(customer_id, name, location, zip, date, description);
	}

	private void init(String customer_id, String name, String location,
			String zip, String date, String description) {
		this.customer_id = customer_id;
		this.name=name;
		this.location = location;
		this.zip = zip;
		this.date = date;
		this.description = description;
	}
	
	public String getCustomer_ID() {
		return customer_id;
	}

	public String getName() {
		return name;
	}

	public String getLocation() {
		return location;
	}
	
	public String getZIP() {
		return zip;
	}
	
	public String getDate() {
		return date;
	}

	public String getDescription() {
		return description;
	}

	public static ArrayList<CDAR_CustomerContractJoin> getEntries() throws Exception {
		ArrayList<CDAR_CustomerContractJoin> join = db.getJoins();
		return join;
	}

}
