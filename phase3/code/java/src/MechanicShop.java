/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class MechanicShop{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public MechanicShop(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}

    public String getMax(String query) throws SQLException{
        Statement stmt = this._connection.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        
        ResultSetMetaData rsmd = rs.getMetaData ();
        int numCol = rsmd.getColumnCount ();
        int rowCount = 0;
        String A = "";
        
        
        while(rs.next()){
            for (int i=1; i<=numCol; ++i)
                A = rs.getString (i);
        }
        stmt.close();
        return A;
    }
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + MechanicShop.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		MechanicShop esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new MechanicShop (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. AddCustomer");
				System.out.println("2. AddMechanic");
				System.out.println("3. AddCar");
				System.out.println("4. InsertServiceRequest");
				System.out.println("5. CloseServiceRequest");
				System.out.println("6. ListCustomersWithBillLessThan100");
				System.out.println("7. ListCustomersWithMoreThan20Cars");
				System.out.println("8. ListCarsBefore1995With50000Milles");
				System.out.println("9. ListKCarsWithTheMostServices");
				System.out.println("10. ListCustomersInDescendingOrderOfTheirTotalBill");
				System.out.println("11. < EXIT");
				
				/*
				 * FOLLOW THE SPECIFICATION IN THE PROJECT DESCRIPTION
				 */
				switch (readChoice()){
					case 1: AddCustomer(esql); break;
					case 2: AddMechanic(esql); break;
					case 3: AddCar(esql); break;
					case 4: InsertServiceRequest(esql); break;
					case 5: CloseServiceRequest(esql); break;
					case 6: ListCustomersWithBillLessThan100(esql); break;
					case 7: ListCustomersWithMoreThan20Cars(esql); break;
					case 8: ListCarsBefore1995With50000Milles(esql); break;
					case 9: ListKCarsWithTheMostServices(esql); break;
					case 10: ListCustomersInDescendingOrderOfTheirTotalBill(esql); break;
					case 11: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice
	
	public static void AddCustomer(MechanicShop esql){//1
		try{


			System.out.print("Enter first name: ");
			String fname = in.readLine();

			System.out.print("Enter last name: ");
			String lname = in.readLine();

			System.out.print("Enter phone number: ");
			String phoneno = in.readLine();

			System.out.print("Enter address: ");
			String address = in.readLine();

			String newCustomer = "INSERT INTO Customer (fname, lname, phone, address) VALUES ( " + "'" + fname + "' , '" + lname + "' , '" + phoneno + "' , '" + address + "')";

			esql.executeUpdate(newCustomer);

		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}
	}
	
	public static void AddMechanic(MechanicShop esql){//2
		try{

	        System.out.print("\tEnter first name: ");
	        String mfname = in.readLine();
	        
	        System.out.print("\tEnter last name: ");
	        String mlname = in.readLine();
	        
	        //System.out.print("\tEnter specialty: ");
	        //String specialty = in.readLine();
	        
	        System.out.print("\tEnter years of experience: ");
	        String experience = in.readLine();
	        
	        String addMechanic = "INSERT INTO Mechanic (fname, lname, experience) VALUES ( " + "'" + mfname + "' , '" + mlname + "' , " + experience + ")";

	        esql.executeUpdate(addMechanic);
		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}		
	}
	
	public static void AddCar(MechanicShop esql){//3
		try{
			System.out.print("\tEnter vin ");
	        String vin = in.readLine();
		    
	        String checkExists = "Select * From Car Where Car.vin = '"+ vin+ "'";
	        if(esql.executeQuery(checkExists)> 0){
	        	System.out.println("ERROR: vin already exists");
	        	return;
	        }
	        System.out.print("\tEnter make: ");
	        String make = in.readLine();
	        
	        System.out.print("\tEnter model: ");
	        String model = in.readLine();

	        System.out.print("\tEnter year: ");
	        String year = in.readLine();
	        
	        String newCar = "INSERT INTO CAR VALUES ( " + "'" + vin + "' , '" + make + "' , '" + model + "' , " + year + ")";
	        
	        esql.executeUpdate(newCar);
	        esql.executeQueryAndPrintResult("Select * From Car Where Car.vin = '"+ vin+ "';");
		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}		
	}
	
	public static void InsertServiceRequest(MechanicShop esql){//4

        try {
        System.out.print("\tEnter last name: ");
        String lname = in.readLine();
        
        String findName = "Select fname, lname, id From Customer Where lname = '" + lname + "'";

        List<List<String>> results = esql.executeQueryAndReturnResult(findName);
        System.out.println ("Results: " + results);
        String choice = "";

        if (esql.executeQuery(findName) == 0) {
        	System.out.print("\tNo customer found");
        	AddCustomer(esql); 
        	return;        
        }
       
		System.out.println ("\tEnter customer id: ");
            String cid = in.readLine();
          
            
        String findAllCars = "Select car_vin From Owns Where customer_id = " + cid;
		List<List<String>> result = esql.executeQueryAndReturnResult(findAllCars);
        System.out.println ("Results: " + result);

          
        choice = "";
        while(!choice.equals("1") && !choice.equals("0"))
        {
            System.out.print("\tChoose 0: Choose car \n\tChoose 1: Add new car\n");
            choice = in.readLine();
        }
        String vin  = "";
            
        if (choice.equals("1"))
        {
        	AddCar(esql);
        	return;

        }
        else if(choice.equals("0"))
        {

            System.out.println ("\tPick your vin: ");
            vin = in.readLine();
        
            System.out.println ("\tInput new service request: ");
            
            System.out.print("\tEnter date (yyyy-mm-dd): ");
            String date = in.readLine();
             
            System.out.print("\tEnter odometer: ");
            String odometer = in.readLine();
             
            System.out.print("\tEnter complaint: ");
            String complaint = in.readLine();
             
            String insertReq = "INSERT INTO Service_Request (customer_id, car_vin, date, odometer, complain) VALUES ( " + "'" + cid + "' , '" + vin + "' , '" + date + "' , '" + odometer + "' , '" + complaint + "')";
            esql.executeUpdate(insertReq);
         }  
            
        } catch(Exception e){
            System.err.println (e.getMessage());
        }
        
	}
	
	public static void CloseServiceRequest(MechanicShop esql) throws Exception {//5
		try{
			System.out.print("\tEmployee ID: ");
			String empID = in.readLine();


			//check if mechanic exist
			String mechanicExists = "Select Mechanic.id From Mechanic Where Mechanic.id = '"+empID+"';";
			if (esql.executeQuery(mechanicExists) > 0){

				System.out.print("Service request number: ");	
				String srn = in.readLine();

				String requestExists = "Select Service_Request.rid From Service_Request Where rid = '"+srn+"';";
				if(esql.executeQuery(requestExists) > 0){

					System.out.println("Closing Date: ");
					String closingDate = in.readLine();

					String closingAfterRequestDate = "Select Service_Request.date From Service_Request,Closed_Request Where Closed_Request.rid = Service_Request.rid and Closed_Request.rid = '"+srn+"' and Closed_Request.date - Service_Request.date < 0;";
					if(esql.executeQuery(closingAfterRequestDate) <= 0){

						System.out.print("Comment: ");
						String comment = in.readLine();

						System.out.print("Bill: ");
						String bill = in.readLine();

						String newClosedRequest = "INSERT INTO Closed_Request (rid, mid, date, comment, bill) VALUES ( " + "'" + srn + "' , '" + empID + "' , '" + closingDate + "' , '" + comment +  "' ,  '"+bill+"');";
						esql.executeUpdate(newClosedRequest);
						return;

					}

					System.out.println("Closing Date is before Request Date");
					return;

				}

				System.out.println("Request does not exist");
					return;


			}

			System.out.println("Mechanic does not exist");
					return;
		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}	
	}
	
	public static void ListCustomersWithBillLessThan100(MechanicShop esql){//6
		try{
			String query = "SELECT date,comment,bill FROM Closed_Request WHERE bill < 100;";
			int rowCount = esql.executeQueryAndPrintResult(query);
			System.out.println("total row(s): " + rowCount);
		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}
	}
	
	public static void ListCustomersWithMoreThan20Cars(MechanicShop esql){//7
		try{
			String query = "SELECT fname, lname FROM Customer,( SELECT customer_id,COUNT(customer_id) as car_num FROM Owns GROUP BY customer_id HAVING COUNT(customer_id) > 20) AS O WHERE O.customer_id = id;";
			int rowCount = esql.executeQueryAndPrintResult(query);
			System.out.println("total row(s): " + rowCount);
		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}	
	}
	
	public static void ListCarsBefore1995With50000Milles(MechanicShop esql){//8
		try{
			String query = "SELECT DISTINCT make,model, year FROM Car AS C, Service_Request AS S WHERE year < 1995 and S.car_vin = C.vin and S.odometer < 50000;";
			int rowCount = esql.executeQueryAndPrintResult(query);
			System.out.println("total row(s): " + rowCount);
		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}	
	}
	
	public static void ListKCarsWithTheMostServices(MechanicShop esql){//9
		try{
			System.out.print("K: ");
			String limit = in.readLine();
			String query = "SELECT make, model, R.creq FROM Car AS C, ( SELECT car_vin, COUNT(rid) AS creq FROM Service_Request GROUP BY car_vin ) AS R WHERE R.car_vin = C.vin ORDER BY R.creq DESC LIMIT "+limit+";";
			int rowCount = esql.executeQueryAndPrintResult(query);
			System.out.println("total row(s): " + rowCount);
		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}
		
	}
	
	public static void ListCustomersInDescendingOrderOfTheirTotalBill(MechanicShop esql){//9
		try{
			String query = "SELECT C.fname , C.lname, Total FROM Customer AS C, (SELECT sr.customer_id, SUM(CR.bill) AS Total FROM Closed_Request AS CR, Service_Request AS SR WHERE CR.rid = SR.rid GROUP BY SR.customer_id) AS A WHERE C.id=A.customer_id ORDER BY A.Total DESC;";
			int rowCount = esql.executeQueryAndPrintResult(query);
			System.out.println("total row(s): " + rowCount);
		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}
		
	}

	
}