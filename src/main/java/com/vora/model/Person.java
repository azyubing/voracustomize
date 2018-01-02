package com.vora.model;

import java.sql.Date;

import scala.Serializable;

public class Person  implements Serializable {

	private static final long serialVersionUID = 1L;
	
	String name;
    Date birthDate;
    
    public Person(String name, Date birthDate) {
    		this.name = name;
    		this.birthDate = birthDate;
    }
    
    public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Date getBirthDate() {
		return birthDate;
	}
	public void setBirthDate(Date birthDate) {
		this.birthDate = birthDate;
	}

}
