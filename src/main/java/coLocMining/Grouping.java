package coLocMining;

public class Grouping {
	
	//private Set<Object> objects;

	public static int sort(Object o1, Object o2) 
	{
	int value = o1.event_type.compareTo(o2.event_type);
	int send = 0;
	if (o1.event_type.equalsIgnoreCase(o2.event_type) ||  value > 0)
	{
		send = 1;
	}
		
		return send;
	
	}
	
	
    public static Object unionObject(Object first, Object second) {
        Object result = null ;
      // result.setObjects(first.getEvent_type().unionAll(second.getEvent_type()));
        return result;
      }
	
}
