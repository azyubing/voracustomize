package com.vora.model;


import scala.Serializable;

public class SalesCase implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	String id;
	Double totalSalesAmount;
    Double noOfItems;
    Integer region;
    
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public Double getTotalSalesAmount() {
		return totalSalesAmount;
	}
	public void setTotalSalesAmount(Double totalSalesAmount) {
		this.totalSalesAmount = totalSalesAmount;
	}
	public Double getNoOfItems() {
		return noOfItems;
	}
	public void setNoOfItems(Double noOfItems) {
		this.noOfItems = noOfItems;
	}
	public Integer getRegion() {
		return region;
	}
	public void setRegion(Integer region) {
		this.region = region;
	}

	@Override
	public String toString() {
		return "SalesCase [id=" + id + ", totalSalesAmount=" + totalSalesAmount + ", noOfItems=" + noOfItems
				+ ", region=" + region + "]";
	}
}
