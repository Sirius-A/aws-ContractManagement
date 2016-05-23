package ch.hsr.bll;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Comparator;

import ch.hsr.dal.CDAR_DatabaseMongoDB;

public class CDAR_Contract implements Comparator<CDAR_Contract> {
	private String id;
	private String description;
	private String date;
	private String refID;

	static CDAR_DatabaseMongoDB db = new CDAR_DatabaseMongoDB();
	
	public CDAR_Contract(String id,String date, String description, String refID) {
		initFromDb(id, date, description, refID);
	}

	public CDAR_Contract(String description, String refID) throws Exception {
		addToDb(description, refID);
	}
	
	private void initFromDb(String id, String date, String description,
			String refID) {
		this.id = id;
		this.date = date;
		this.description = description;
		this.refID = refID;
	}

	private void addToDb(String description, String refID) throws Exception {
		this.date = new Date(System.currentTimeMillis()).toString();
		this.description = description;
		this.refID = refID;
		db.addEntry(this);
	}
	
	public String getID() {
		return id;
	}
	
	public String getDate() {
		return date;
	}

	public String getDescription() {
		return description;
	}

	public String getRefID() {
		return refID;
	}
	
	public static ArrayList<CDAR_Contract> getEntries() throws Exception {
		ArrayList<CDAR_Contract> contract = db.getContracts();
		return contract;
	}

	@Override
	public int compare(CDAR_Contract o1, CDAR_Contract o2) {
		return Integer.parseInt(((CDAR_Contract) o1).getID())
				- Integer.parseInt(((CDAR_Contract) o2).getID());
	}
}
