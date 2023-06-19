package excel2db;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;

import java.sql.*;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.HashSet;

import com.google.protobuf.MapEntry;
import com.mysql.jdbc.Driver;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.xmlbeans.impl.xb.xsdschema.MaxExclusiveDocument;


public class Excel2db {

	public static void main(String[] args) {
		
		POI2MySQL tp = new POI2MySQL();
		//tp.convertExcelDoc();
		tp.scrape_v2_2_tables();
	}
	

}

class POI2MySQL{
	
	static String fname = "DevSecOpsActivitesToolsGuidebookTables.xlsx";
	
	static String excel2db_version = 
// version			
"20230619-1";
// version
	static String xlsx_version = "V2.2 25-05-23";
	
	
	void scrape_v2_2_tables() {
		
		HashMap<String, Sheet> phaseSheets = new HashMap<String, Sheet>() ;

		Set<String> phaseSet = new HashSet<String>(Arrays.asList(new String[] {"Plan","Develop","Build","Test","Release","Deliver","Deploy","Operate","Monitor","Feedback"}));

		
		try {
			FileInputStream fis = new FileInputStream(fname);
			XSSFWorkbook wb = new XSSFWorkbook(fis);
	        for (int i = 0; i < wb.getNumberOfSheets(); i++) {
	            Sheet sheet = wb.getSheetAt(i);
	            String shName = sheet.getSheetName();
	            if (phaseSet.contains(shName)) {
	            	phaseSheets.put(shName, sheet);
	            }     
	        }

            
            
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println(phaseSheets.size());
		for ( Entry<String, Sheet> ess : phaseSheets.entrySet() ) {
			System.out.println(ess.getKey());
			System.out.println(ess.getValue().getSheetName());
			System.out.println();
		}
	}
	
	void convertExcelDoc() {
		

		ArrayList<String[]> toolRows = new ArrayList<String[]>();
		ArrayList<String[]> activityRows = new ArrayList<String[]>();
		HashMap<String[],ArrayList<String>> activityInputs = new HashMap();
		HashMap<String[],ArrayList<String>> activityOutputs = new HashMap();
		HashMap<String[],ArrayList<String>> activityTools = new HashMap();
		HashMap<String[],ArrayList<String>> toolInputs = new HashMap();
		HashMap<String[],ArrayList<String>> toolOutputs = new HashMap();
		
		populateToolsActivitiesLists(toolRows, activityRows);
		
		normaliseLists(toolRows, activityRows, activityInputs, activityOutputs, activityTools, toolInputs, toolOutputs );
		
		insertToDb(toolRows, activityRows, activityInputs, activityOutputs, activityTools, toolInputs, toolOutputs);
		
		
		
		System.out.println("OK");
		
		
	}
	
	private void normaliseLists(ArrayList<String[]> toolRows, ArrayList<String[]> activityRows,
			HashMap<String[],ArrayList<String>> activityInputs,
			HashMap<String[],ArrayList<String>> activityOutputs ,
			HashMap<String[],ArrayList<String>> activityTools ,
			HashMap<String[],ArrayList<String>> toolInputs ,
			HashMap<String[],ArrayList<String>> toolOutputs) {
		

		
		for ( String[] ac : activityRows ) {
			ac[3] = ac[3].toLowerCase();
			ac[4] = ac[4].toLowerCase();
			ac[5] = ac[5].toLowerCase();
			
			ArrayList<String> acI = new ArrayList<String>();
			ArrayList<String> acO  = new ArrayList<String>();
			ArrayList<String> acT  = new ArrayList<String>();
			
			activityInputs.put(ac, acI);
			activityOutputs.put(ac, acO);
			activityTools.put(ac, acT);
			extractTokens(ac[3], acT);
			extractTokens(ac[4], acI);
			extractTokens(ac[5], acO);
			
		}
		for ( String[] t : toolRows ) {
			t[1] = t[1].toLowerCase();
			t[5] = t[5].toLowerCase();
			t[6] = t[6].toLowerCase();
			
			ArrayList<String> tI = new ArrayList<String>();
			ArrayList<String> tO  = new ArrayList<String>();
			
			toolInputs.put(t, tI);
			toolOutputs.put(t ,tO);
			extractTokens(t[5], tI);
			extractTokens(t[6], tO);
			
			
		}
		
	}

	private void extractTokens(String inStr, ArrayList<String> out) {
		System.out.println(inStr);
		Arrays.asList(inStr.split("\n|;|\\.|,| or ")).stream().forEach(s -> 
				{
					String strim = s.replaceAll("^-","").trim();
					System.out.printf("String:'%s', len:%d \n", strim, strim.length());
					if ( strim.length() > 0 ) out.add(strim);
					
					
				});
		
	}

	void insertToDb( ArrayList<String[]> toolRows, ArrayList<String[]> activityRows, HashMap<String[],ArrayList<String>> activityInputs, HashMap<String[],ArrayList<String>> activityOutputs, HashMap<String[],ArrayList<String>> activityTools, HashMap<String[],ArrayList<String>> toolInputs, HashMap<String[],ArrayList<String>> toolOutputs ) {
		try {

			Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/dsopguidebooktablesraw","devopstools","devopstools");
			Statement stmt = con.createStatement();
			stmt.executeUpdate("truncate tools");
			stmt.executeUpdate("truncate activities");
			stmt.executeUpdate("truncate all_artifacts");
			stmt.executeUpdate("truncate all_tools");
			
			stmt.executeUpdate("truncate source_info");
			stmt.executeUpdate("insert into source_info VALUES ('" + xlsx_version + "','" + excel2db_version + "' )");
			
			
			PreparedStatement pstmt = con.prepareStatement("insert into activities  values (?,?,?,?,?,?,?,?)");
			int srcorder = 1;
			for ( String[] sa : activityRows) {
				pstmt = con.prepareStatement("insert into activities  values (?,?,?,?,?,?,?,?)");
				for (int i = 0; i < 6; i++) {
					pstmt.setString(i+1,sa[i]);	
				}
				pstmt.setInt(7,srcorder);
				pstmt.setInt(8,0); // data_cleansed = 0
				pstmt.executeUpdate();
				
				
				ArrayList<String> aI = activityInputs.get(sa);
				ArrayList<String> aO = activityOutputs.get(sa);
				ArrayList<String> aT = activityTools.get(sa);
				
				pstmt = con.prepareStatement("insert into all_artifacts values (?,?,?,?,?,?,?)");
				for ( String s : aI) {
					pstmt.setString(1, s);
					pstmt.setString(2, "Activity");
					pstmt.setString(3, "Input");
					pstmt.setString(4, sa[0]);
					pstmt.setString(5, sa[1]);
					pstmt.setInt(6,srcorder);
					pstmt.setInt(7,0); // data_cleansed = 0
					pstmt.executeUpdate();
				}
				
				pstmt = con.prepareStatement("insert into all_artifacts values (?,?,?,?,?,?,?)");
				for ( String s : aO) {
					pstmt.setString(1, s);
					pstmt.setString(2, "Activity");
					pstmt.setString(3, "Output");
					pstmt.setString(4, sa[0]);
					pstmt.setString(5, sa[1]);
					pstmt.setInt(6,srcorder);
					pstmt.setInt(7,0); // data_cleansed = 0
					pstmt.executeUpdate();
				}
				
				pstmt = con.prepareStatement("insert into all_tools values (?,?,?,?,?,?)");
				for ( String s : aT) {
					pstmt.setString(1, s);
					pstmt.setString(2, "Activities");
					pstmt.setString(3, sa[0]);
					pstmt.setString(4, sa[1]);
					pstmt.setInt(5,srcorder);
					pstmt.setInt(6,0); // data_cleansed = 0
					pstmt.executeUpdate();
				}
				srcorder++;
				
			}
						
			srcorder = 1;
			for ( String[] sa : toolRows) {
				pstmt = con.prepareStatement("insert into tools  values (?,?,?,?,?,?,?,?,?)");
				for (int i = 0; i < 7; i++) {
					pstmt.setString(i+1,sa[i]);	
				}
				pstmt.setInt(8,srcorder);
				pstmt.setInt(9,0); // data_cleansed = 0
				pstmt.executeUpdate();

				pstmt = con.prepareStatement("insert into all_tools values (?,?,?,?,?,?)");
				pstmt.setString(1, sa[1].toLowerCase());
				pstmt.setString(2, "Tools");
				pstmt.setString(3, sa[0]);
				pstmt.setString(4, sa[1]);
				pstmt.setInt(5,srcorder);
				pstmt.setInt(6,0); // data_cleansed = 0
				pstmt.executeUpdate();
			
				ArrayList<String> tI = toolInputs.get(sa);
				ArrayList<String> tO = toolOutputs.get(sa);
				
				pstmt = con.prepareStatement("insert into all_artifacts values (?,?,?,?,?,?,?)");
				for ( String s : tI) {
					pstmt.setString(1, s);
					pstmt.setString(2, "Tool");
					pstmt.setString(3, "Input");
					pstmt.setString(4, sa[0]);
					pstmt.setString(5, sa[1]);
					pstmt.setInt(6,srcorder);
					pstmt.setInt(7,0); // data_cleansed = 0
					pstmt.executeUpdate();
				}
				
				pstmt = con.prepareStatement("insert into all_artifacts values (?,?,?,?,?,?,?)");
				for ( String s : tO) {
					pstmt.setString(1, s);
					pstmt.setString(2, "Tool");
					pstmt.setString(3, "Output");
					pstmt.setString(4, sa[0]);
					pstmt.setString(5, sa[1]);
					pstmt.setInt(6,srcorder);
					pstmt.setInt(7,0); // data_cleansed = 0
					pstmt.executeUpdate();
				}
				


				srcorder++;
				
			}

			
			
			
			
			
			con.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	int[][] toolRowFormat = {
			{1,2,3,4,-1,-1}, // inputs outputs not present
			{1,2,3,5,4,-1},  // outputs not present
			{1,2,3,5,6,4}
	}; 
	int[][] activityRowFormat =  {
			{1,2,3,-1,-1}, // inputs outputs not present
			{1,2,4,3,-1},  // outputs not present
			{1,2,4,5,3}
	}; 
	
	void populateToolsActivitiesLists(ArrayList<String[]> toolRows, ArrayList<String[]> activityRows ) {
		
		try {
			FileInputStream fis = new FileInputStream(fname);
			XSSFWorkbook wb = new XSSFWorkbook(fis);
            Sheet datatypeSheet = wb.getSheetAt(0);
            xlsx_version = datatypeSheet.getSheetName();
            Iterator<Row> iterator = datatypeSheet.iterator();

            while (iterator.hasNext()) {

                Row currentRow = iterator.next();
                if (currentRow.getFirstCellNum() == 0) {
                	boolean isToolRow = currentRow.getCell(0).getStringCellValue().equals("Tools");
                	boolean isActivityRow = !isToolRow;

                	int none_i_io_format = (int) currentRow.getCell(1).getNumericCellValue();
                	
                	String phase = currentRow.getCell(2).getStringCellValue();
                	
                	//System.out.printf("%s,  %d, %s \n",String.valueOf(isToolRow), none_i_io_format, phase);
                	
                	if (isToolRow) {
                	  String[] toolRow = {"","","","","","","",};
                	  toolRow[0]=phase;
                	  int formatIdx = 0;
                      Iterator<Cell> cellIterator = currentRow.iterator();
                      cellIterator.next();cellIterator.next();cellIterator.next();
                      while (cellIterator.hasNext()) {
                        Cell currentCell = cellIterator.next();
                          
      					if (currentCell.getCellType() == CellType.STRING) {
      						toolRow[toolRowFormat[none_i_io_format][formatIdx]]=currentCell.getStringCellValue();
          					formatIdx++;
      					} 

                      }
                      toolRows.add(toolRow);

                	}
                	else if (isActivityRow) {
                  	  String[] activityRow = {"","","","","","",};
                  	  activityRow[0]=phase;
                  	  int formatIdx = 0;
                        Iterator<Cell> cellIterator = currentRow.iterator();
                        cellIterator.next();cellIterator.next();cellIterator.next();
                        while (cellIterator.hasNext()) {
                          Cell currentCell = cellIterator.next();
                            
        					if (currentCell.getCellType() == CellType.STRING) {
        						activityRow[activityRowFormat[none_i_io_format][formatIdx]]=currentCell.getStringCellValue();
            					formatIdx++;
        					} 

                        }
                        activityRows.add(activityRow);
             		
                	}
                	
                }
                
                

            }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
}