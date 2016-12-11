package storm.workload_prediction;

public class VM implements java.io.Serializable {

	String name;
	String id;
	public  VM(String id,String name) {
		this.id=id;
		this.name=name;
	} 
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String toString() {
		return "name:"+name+" id:"+id;
	}
}
