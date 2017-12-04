package com.oracle.hadoop.employee;

public abstract class SalarizedEmp implements Employee {

	double salary;

	public double getSalary() {
		return salary;
	}

	public void setSalary(double salary) {
		this.salary = salary;
	}

}
