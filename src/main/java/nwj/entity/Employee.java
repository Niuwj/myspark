package nwj.entity;

import java.io.Serializable;

public class Employee implements Serializable {
	//一个类的对象要想序列化成功，必须满足两个条件：
	//该类必须实现 java.io.Serializable 对象。
	//该类的所有属性必须是可序列化的。如果有一个属性不是可序列化的，则该属性必须注明是短暂的。
	public String name;
	public String address;
	public transient int SSN;
	public int number;
	public void mailCheck()
	{
		System.out.println("Mailing a check to " + name
				+ " " + address);
	}
}
