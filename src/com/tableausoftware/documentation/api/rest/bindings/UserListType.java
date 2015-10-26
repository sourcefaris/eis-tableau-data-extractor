package com.tableausoftware.documentation.api.rest.bindings;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "userListType", propOrder = { "user" })
public class UserListType {
	protected List<UserType> user;
	
	public List<UserType> getUser() {
		if (user == null) {
            user = new ArrayList<UserType>();
        }
		return this.user;
	}

}
