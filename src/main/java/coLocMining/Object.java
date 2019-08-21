package coLocMining;

public class Object {
  String event_type;
  int instance_id;
  double x;
  double y;
public Object(String event_type, int instance_id, double x, double y) {
		super();
		this.event_type = event_type;
		this.instance_id = instance_id;
		this.x = x;
		this.y = y;
	}
public String getEvent_type() {
	return event_type;
}
public void setEvent_type(String event_type) {
	this.event_type = event_type;
}

public int getInstance_id() {
	return instance_id;
}
public void setInstance_id(int instance_id) {
	this.instance_id = instance_id;
}
public double getX() {
	return x;
}
public void setX(double x) {
	this.x = x;
}
public double getY() {
	return y;
}
public void setY(double y) {
	this.y = y;
}
  
}
