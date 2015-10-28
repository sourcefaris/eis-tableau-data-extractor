package com.eis.dataextractor.main;

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
import com.eis.dataextractor.api.rest.bindings.DataSourceType;
import com.eis.dataextractor.api.rest.bindings.ProjectListType;
import com.eis.dataextractor.api.rest.bindings.ProjectType;
import com.eis.dataextractor.api.rest.bindings.TableauCredentialsType;
import com.eis.dataextractor.utils.RestApiUtils;
import com.eis.dataextractor.utils.StringDateUtils;
import com.tableausoftware.TableauException;
import com.tableausoftware.DataExtract.Collation;
import com.tableausoftware.DataExtract.Extract;
import com.tableausoftware.DataExtract.Row;
import com.tableausoftware.DataExtract.Table;
import com.tableausoftware.DataExtract.TableDefinition;
import com.tableausoftware.DataExtract.Type;

public class SubscriberPrepaid extends LocalDBController {
	private static List<String> dateCreates = new ArrayList<>();
    private static final RestApiUtils s_restApiUtils = RestApiUtils.getInstance();
    static Timestamp times = null;
	// Define the table's schema
	private static TableDefinition makeTableDefinition() throws TableauException {
		TableDefinition tableDef = new TableDefinition();
		tableDef.setDefaultCollation(Collation.EN_GB);
		tableDef.addColumn("actvn_dt", Type.CHAR_STRING);
		tableDef.addColumn("period_date", Type.CHAR_STRING);
		tableDef.addColumn("brnd_id", Type.CHAR_STRING);
		tableDef.addColumn("spn_tnr_range", Type.CHAR_STRING);
		tableDef.addColumn("promo_package_name", Type.CHAR_STRING);
		tableDef.addColumn("hlr_region", Type.CHAR_STRING);
		tableDef.addColumn("hlr_branch", Type.CHAR_STRING);
		tableDef.addColumn("hlr_city", Type.CHAR_STRING);
		tableDef.addColumn("cluster_type", Type.CHAR_STRING);
		tableDef.addColumn("ar_lcs_tp_nm", Type.CHAR_STRING);
		tableDef.addColumn("Num_of_Subs", Type.DOUBLE);
		tableDef.addColumn("Num_of_Acc", Type.DOUBLE);
		tableDef.addColumn("TIMESTAMP", Type.CHAR_STRING);
		return tableDef;
	}

	/*
	 * Insert Subscriber Prepaid
	 */
	public static void insertData(Table table) {
		String querySql = "select "+
				"dm_subscriber_prepaid.period_date,"
				+"dm_subscriber_prepaid.brnd_id,"
				+"dm_subscriber_prepaid.spn_tnr_range,"
				+"dm_subscriber_prepaid.promo_package_name,"
				+"dm_subscriber_prepaid.hlr_region,"
				+"dm_subscriber_prepaid.hlr_branch,"
				+"dm_subscriber_prepaid.hlr_city,"
				+"dm_subscriber_prepaid.cluster_type,"
				+"dm_subscriber_prepaid.ar_lcs_tp_nm,"
				+"sum(dm_subscriber_prepaid.num_of_subs) Num_of_Subs,"
				+"sum(dm_subscriber_prepaid.acc_bal) Num_of_Acc "
				+"from eis.dm_subscriber_prepaid WHERE dm_subscriber_prepaid.period_date LIKE '2014%' "
				+"group by "
				+"dm_subscriber_prepaid.period_date,"
				+"dm_subscriber_prepaid.brnd_id,"
				+"dm_subscriber_prepaid.spn_tnr_range,"
				+"dm_subscriber_prepaid.promo_package_name,"
				+"dm_subscriber_prepaid.hlr_region,"
				+"dm_subscriber_prepaid.hlr_branch,"
				+"dm_subscriber_prepaid.hlr_city,"
				+"dm_subscriber_prepaid.cluster_type,"
				+"dm_subscriber_prepaid.ar_lcs_tp_nm "
				+"order by dm_subscriber_prepaid.period_date asc";
		Timestamp ts = getMaxDateTime("max_date.sales_prepaid");
		try {
			TableDefinition tableDef = table.getTableDefinition();
			Row row = new Row(tableDef);
			Class.forName(getPropertyValue("hadoop.driver"));
			Connection connection = DriverManager.getConnection(getPropertyValue("hadoop.connection"),getPropertyValue("hadoop.username"),getPropertyValue("hadoop.password"));
			Statement stat = connection.createStatement();
			ResultSet rs = stat.executeQuery(querySql);
			int i = 1;
			while (rs.next()) {
				row.setCharString(0, rs.getString("period_date")!=null?rs.getString("period_date"):"0000-00-00");
				row.setCharString(1, rs.getString("brnd_id")!=null?rs.getString("brnd_id"):"");
				row.setCharString(2, rs.getString("spn_tnr_range")!=null?rs.getString("spn_tnr_range"):"");
				row.setCharString(3, rs.getString("promo_package_name")!=null?rs.getString("promo_package_name"):"");
				row.setCharString(4, rs.getString("hlr_region")!=null?rs.getString("hlr_region"):"");
				row.setCharString(5, rs.getString("hlr_branch")!=null?rs.getString("hlr_branch"):"");
				row.setCharString(6, rs.getString("hlr_city")!=null?rs.getString("hlr_city"):"");
				row.setCharString(7, rs.getString("cluster_type")!=null?rs.getString("cluster_type"):"");
				row.setCharString(8, rs.getString("ar_lcs_tp_nm")!=null?rs.getString("ar_lcs_tp_nm"):"");
				row.setDouble(9, rs.getDouble("Num_of_Subs"));
				row.setDouble(10, rs.getDouble("Num_of_Acc"));
				row.setCharString(11, new Timestamp(System.currentTimeMillis())+"");
//					dateCreates.add(rs.getString(18));
				table.insert(row);
				System.out.println(i+i>1?" rows":" row"+" inserted "+new Timestamp(System.currentTimeMillis())+" | "+times);
				i++;
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
        String datasourceName = getPropertyValue("tableau.datasource.subscriber_prepaid.name");
        
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
        String datasourceFileName = getPropertyValue("tableau.datasource.subscriber_prepaid.file");
        times = new Timestamp(System.currentTimeMillis());
		try (Extract extract = new Extract(datasourceFileName)) {
			Table table;
			System.out.println(extract.hasTable("Extract"));
			if (!extract.hasTable("Extract")) {
				TableDefinition tableDef = makeTableDefinition();
				table = extract.addTable("Extract", tableDef);
			} else {
				table = extract.openTable("Extract");
			}

			TableDefinition tableDef = table.getTableDefinition();
			printTableDefinition(tableDef);

//			insertData(table);
//			if(dateCreates!=null&&!dateCreates.isEmpty()){
//				updateProperty(StringDateUtils.convertDateToString(StringDateUtils.getMaxDate(dateCreates)), "max_date.sales_prepaid");
//			} 
			publishDataSource(datasourceFileName);
			
		} catch (TableauException e) {
			e.printStackTrace();
		}
	}
	
}
