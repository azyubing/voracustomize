package com.vora.model;


import scala.Serializable;

public class SalesCaseKmViewModel  implements Serializable {

	private static final long serialVersionUID = 1L;
	String id;
    String cluster;
    
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getCluster() {
		return cluster;
	}
	public void setCluster(String cluster) {
		this.cluster = cluster;
	}


}
