package ch.hsr.pl;

import javax.servlet.http.*;
import javax.servlet.*;

import ch.hsr.bll.*;

import java.io.*;
import java.lang.invoke.WrongMethodTypeException;
import java.sql.SQLException;
import java.util.ArrayList;

public class CDAR_ContractManagementServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	public void init(ServletConfig aConfig) throws ServletException {
		super.init(aConfig);
	}

	public void doGet(HttpServletRequest req, HttpServletResponse res)
			throws IOException {

		try {
			printSite(req, res);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void doPost(HttpServletRequest req, HttpServletResponse res)
			throws IOException {
		res.setContentType("text/html");
		try {
			if (req.getParameter("Customer") != null)
				addCustomer(req);
			else
				addContract(res, req);

			printSite(req, res);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void printSite(HttpServletRequest req, HttpServletResponse res)
			throws IOException, Exception {
		printHeader(req, res);
		printDBResult(res, req);
		printFooter(res);
	}

	protected void printHeader(HttpServletRequest req, HttpServletResponse res)
			throws IOException {
		PrintWriter out = res.getWriter();
		out.println("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.1//EN\" \"http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd\">");
		out.println("<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\" lang=\"de\">");
		out.println("<head>");
		out.println("<meta http-equiv=\"Content-Type\" content=\"text/html;charset=utf-8\" />");
		out.println("<title>ContractManagement_MongoDB-Applikation</title>");
		out.println("<script type=\"text/javascript\" src=\"" + req.getContextPath() + "/js/validate.js\"></script>");
		//Fehler in der Anzeige aufgrund der zu langen id's
		//out.println("<LINK href=\"" + req.getContextPath() + "/css/style.css" + "\" rel=\"stylesheet\" type=\"text/css\">");
		out.println("</head>");
		out.println("<body>");
		out.println("<h1>ContractManagement_Datastore-Applikation</h1>");
	}

	protected void printFooter(HttpServletResponse res) throws IOException {
		PrintWriter out = res.getWriter();
		out.println("</body>");
		out.println("</html>");
	}

	private void printDBResult(HttpServletResponse res, HttpServletRequest req)
			throws Exception {
		PrintWriter out = res.getWriter();
		out.println("<div id=\"container\"><div id=\"box1\">");
		out.println("<table><tr><td>");
		printCustomerTable(res, req);
		out.println("</td></tr></table></div><div id=\"box2\"><table><tr><td>");
		printContractTable(res, req, CDAR_Contract.getEntries());

		out.println("</td></tr></table></div><div id=\"box3\"><table><tr><td>");
		printJoinTable(res, req, CDAR_CustomerContractJoin.getEntries());

		out.println("</div></td></tr></table></div>");
	}

	protected void printCustomerTable(HttpServletResponse res,
			HttpServletRequest req)
			throws Exception {
		PrintWriter out = res.getWriter();
		ArrayList<CDAR_Customer> dbResult = CDAR_Customer.getEntries();
		out.println("<h3>Customer</h3>");
		out.println("<table border=\"1\" id=\"customerTable\">");
		out.println("<tr><td>id</td><td>name</td><td>location</td><td>zip</td></tr>");
		for (CDAR_Customer cus : dbResult) {
			out.println("<tr>");
			out.println("<td>" + cus.getID() + "</td>");
			out.println("<td>" + cus.getName() + "</td>");
			out.println("<td>" + cus.getLocation() + "</td>");
			out.println("<td>" + cus.getZip() + "</td>");
			out.println("</tr>");
		}
		out.println("<form method=\"post\" action=\""
				+ removeQueryString(req.getRequestURI())
				+ "\" onsubmit=\"return customerValidateForm(this)\"><td></td><td>"
				+ "<input type=\"text\" name=\"nameCustomer\" size=\"15\" maxlength=\"60\" /></td>"
				+ "<td><input type=\"text\" name=\"locationCustomer\" size=\"15\" maxlength=\"60\" /></td>"
				+ "<td><input type=\"text\" name=\"zipCustomer\" size=\"15\" maxlength=\"30\" /></td>"
				+ "<tr><td></td><td colspan = 3 align=\"right\"><input type=\"submit\" name=\"Customer\" value=\"Add entry.\" />"
				+ "</td></tr>" + "</form>");

		out.println("</tr></table>");
	}

	protected void printContractTable(HttpServletResponse res,
			HttpServletRequest req, ArrayList<CDAR_Contract> dbResult)
			throws IOException, ClassNotFoundException, SQLException {
		PrintWriter out = res.getWriter();
		out.println("<h3>Contract</h3>");

		out.println("<table border=\"1\"><tr><td>id</td><td>date</td><td>description</td><td>customer id</td></tr>");
		for (CDAR_Contract con : dbResult) {
			out.println("<tr>");
			out.println("<td>" + con.getID() + "</td>");
			out.println("<td>" + con.getDate() + "</td>");
			out.println("<td>" + con.getDescription() + "</td>");
			out.println("<td>" + con.getRefID() + "</td>");
			out.println("</tr>");
		}
		out.println("<form method=\"post\" action=\""
				+ removeQueryString(req.getRequestURI())
				+ "\" onsubmit=\"return contractValidateForm(this)\"><td></td><td></td><td>"
				+ "<input type=\"text\" name=\"descriptionContract\" size=\"15\" maxlength=\"60\" /></td>"
				+ "<td><input type=\"text\" name=\"refIDContract\" size=\"15\" maxlength=\"60\" /></td>"
				+ "<tr><td></td><td></td><td colspan =2 align=\"right\"><input type=\"submit\" name=\"Contract\"  value=\"Add entry.\" />"
				+ "</td></tr>" + "</form>");

		out.println("</tr></table>");
	}

	private void printJoinTable(HttpServletResponse res,
			HttpServletRequest req, ArrayList<CDAR_CustomerContractJoin> dbResult)
			throws IOException {
		PrintWriter out = res.getWriter();
		out.println("<h3>Join Customer - Contract</h3>");
		out.println("<table border=\"1\">");
		for (CDAR_CustomerContractJoin join : dbResult) {
			out.println("<tr>");
			out.println("<td>" + join.getCustomer_ID() + "</td>");
			out.println("<td>" + join.getName() + "</td>");
			out.println("<td>" + join.getLocation() + "</td>");
			out.println("<td>" + join.getZIP() + "</td>");
			out.println("<td>" + join.getDate() + "</td>");
			out.println("<td>" + join.getDescription() + "</td>");
			out.println("</tr>");
		}
		out.println("</table>");
	}

	private static String removeQueryString(String aURI) {
		int i = aURI.indexOf('?');
		if (i != -1) {
			aURI = aURI.substring(0, i);
		}
		return aURI;
	}

	private void addCustomer(HttpServletRequest req) {
		new CDAR_Customer(req.getParameter("nameCustomer"),
				req.getParameter("locationCustomer"),
				req.getParameter("zipCustomer"));
	}

	private void addContract(HttpServletResponse res, HttpServletRequest req) throws IOException {
		try {
			new CDAR_Contract(req.getParameter("descriptionContract"),
					req.getParameter("refIDContract"));
		} catch (Exception e) {
			PrintWriter out =res.getWriter();
			out.println("<script type=\"text/javascript\">");  
			out.println("alert('Ungültige Referenznummer');");
			out.println("</script>");
			}		
	}
}