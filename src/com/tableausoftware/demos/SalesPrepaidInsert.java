package com.tableausoftware.demos;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import com.eis.dataextractor.LocalDBController;
import com.tableausoftware.TableauException;
import com.tableausoftware.DataExtract.Collation;
import com.tableausoftware.DataExtract.Extract;
import com.tableausoftware.DataExtract.Row;
import com.tableausoftware.DataExtract.Table;
import com.tableausoftware.DataExtract.TableDefinition;
import com.tableausoftware.DataExtract.Type;
import com.tableausoftware.documentation.api.rest.bindings.DataSourceType;
import com.tableausoftware.documentation.api.rest.bindings.ProjectListType;
import com.tableausoftware.documentation.api.rest.bindings.ProjectType;
import com.tableausoftware.documentation.api.rest.bindings.TableauCredentialsType;
import com.tableausoftware.documentation.api.rest.util.RestApiUtils;
import com.tableausoftware.documentation.api.rest.util.StringDateUtils;

public class SalesPrepaidInsert extends LocalDBController {
	private static List<String> dateCreates = new ArrayList<>();
    private static final RestApiUtils s_restApiUtils = RestApiUtils.getInstance();

	// Define the table's schema
	private static TableDefinition makeTableDefinition() throws TableauException {
		TableDefinition tableDef = new TableDefinition();
		tableDef.setDefaultCollation(Collation.EN_GB);
		tableDef.addColumn("ACTVN_DT", Type.DATE);
		tableDef.addColumn("BRND_ID", Type.CHAR_STRING);
		tableDef.addColumn("DISTRO_AREA", Type.CHAR_STRING);
		tableDef.addColumn("DISTRO_SALES_AREA", Type.CHAR_STRING);
		tableDef.addColumn("DISTRO_CLUSTER", Type.CHAR_STRING);
		tableDef.addColumn("TLD_NAME", Type.CHAR_STRING);
		tableDef.addColumn("PROMO_PACKAGE_NAME", Type.CHAR_STRING);
		tableDef.addColumn("HLR_REGION", Type.CHAR_STRING);
		tableDef.addColumn("HLR_BRANCH", Type.CHAR_STRING);
		tableDef.addColumn("HLR_CITY", Type.CHAR_STRING);
		tableDef.addColumn("CLUSTER_TYPE", Type.CHAR_STRING);
		tableDef.addColumn("CA_REGION", Type.CHAR_STRING);
		tableDef.addColumn("CA_AREA", Type.CHAR_STRING);
		tableDef.addColumn("CA_SALES_AREA", Type.CHAR_STRING);
		tableDef.addColumn("CA_CLUSTER", Type.CHAR_STRING);
		tableDef.addColumn("NUM_OF_SALES", Type.CHAR_STRING);
		tableDef.addColumn("PNAME", Type.CHAR_STRING);
		tableDef.addColumn("TIMESTAMP", Type.CHAR_STRING);
		return tableDef;
	}

	/*
	 * Insert Sales Prepaid
	 */
	public static void insertData(Table table) {
		String quertySql = "SELECT * FROM dm_sales_prepaid";
		Timestamp ts = getMaxDateTime(SALES_PREPAID);
		try {
			TableDefinition tableDef = table.getTableDefinition();
			Row row = new Row(tableDef);
			Class.forName("com.mysql.jdbc.Driver");
			Connection connection = DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/eis_data_extractor","root", "");
			Statement stat = connection.createStatement();
			ResultSet rs = stat.executeQuery(quertySql);
			while (rs.next()) {
				if(StringDateUtils.convertStringToDate(rs.getString(18)).after(ts)){
					row.setDate(0, Integer.parseInt(rs.getString(1).split("-")[0]), Integer.parseInt(rs.getString(1).split("-")[1]), Integer.parseInt(rs.getString(1).split("-")[2]));
					row.setCharString(1, rs.getString(2));
					row.setCharString(2, rs.getString(3));
					row.setCharString(3, rs.getString(4));
					row.setCharString(4, rs.getString(5));
					row.setCharString(5, rs.getString(6));
					row.setCharString(6, rs.getString(7));
					row.setCharString(7, rs.getString(8));
					row.setCharString(8, rs.getString(9));
					row.setCharString(9, rs.getString(10));
					row.setCharString(10, rs.getString(11));
					row.setCharString(11, rs.getString(12));
					row.setCharString(12, rs.getString(13));
					row.setCharString(13, rs.getString(14));
					row.setCharString(14, rs.getString(15));
					row.setCharString(15, rs.getString(16));
					row.setCharString(16, rs.getString(17));
					row.setCharString(17, rs.getString(18));
					dateCreates.add(rs.getString(18));
					table.insert(row);
				}
			}
			System.out.println("Connection success");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (TableauException e) {
			e.printStackTrace();
		}
	}
	
	// Print a Table's schema to stderr.
	private static void printTableDefinition(TableDefinition tableDef) throws TableauException {
		int numColumns = tableDef.getColumnCount();
		for (int i = 0; i < numColumns; ++i) {
			Type type = tableDef.getColumnType(i);
			String name = tableDef.getColumnName(i);
			System.out.format("Column %d: %s (%#06x)\n", i, name, type.getValue());
		}
	}
	
	/* 
	 * Publish DataSource
	 */
	public static void publishDataSource(String datasourceFileName){
        String username = getPropertyValue("tableau.username");
        String password = getPropertyValue("tableau.password");
        String contentUrl = getPropertyValue("tableau.siteUrl");
        String datasourceName = "Sales Prepaid";
        
        File datasourceFile = new File(datasourceFileName);
        
        TableauCredentialsType credential = s_restApiUtils.invokeSignIn(username, password, contentUrl);
        String currentSiteId = credential.getSite().getId();
        
        ProjectType defaultProject = null;
        ProjectListType projects = s_restApiUtils.invokeQueryProjects(credential, currentSiteId);
        for (ProjectType project : projects.getProject()) {
            if (project.getName().equals("default")) {
                defaultProject = project;
                System.out.println("Default project found: %s" + defaultProject.getId());
            }
        }
		DataSourceType publishDatasource = s_restApiUtils.invokePublishDataSource(credential, defaultProject.getId(), datasourceName, datasourceFile);
		System.out.println("Successfully Publish datasource "+publishDatasource.getName()+"("+publishDatasource.getId()+")");
	}
	
	public static void main(String[] args) {
        String datasourceFileName = "tde/SalesPrepaid.tde";
        
		try (Extract extract = new Extract(datasourceFileName)) {
			Table table;
			if (!extract.hasTable("Extract")) {
				TableDefinition tableDef = makeTableDefinition();
				table = extract.addTable("Extract", tableDef);
			} else {
				table = extract.openTable("Extract");
			}

			TableDefinition tableDef = table.getTableDefinition();
			printTableDefinition(tableDef);

			insertData(table);
			if(dateCreates!=null&&!dateCreates.isEmpty())
				updateProperty(StringDateUtils.convertDateToString(StringDateUtils.getMaxDate(dateCreates)), SALES_PREPAID);
			publishDataSource(datasourceFileName);
			
		} catch (TableauException e) {
			e.printStackTrace();
		}
	}
	
}
