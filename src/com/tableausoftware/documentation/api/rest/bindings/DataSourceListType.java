package com.tableausoftware.documentation.api.rest.bindings;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "dataSourceListType", propOrder = { "datasource" })
public class DataSourceListType {
	protected List<DataSourceType> datasource;
	
	public List<DataSourceType> getDatasource() {
		if(datasource == null){
			datasource = new ArrayList<DataSourceType>();
		}
		return this.datasource;
	}
}
