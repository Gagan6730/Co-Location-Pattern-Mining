package coLocMining;
import java.awt.Point;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Read_Points {
	static Point[] points = null;
	
	public static Point[] readTextFileUsingScanner(String fileName) 
	{ 
	try 
	{ 
	Scanner sc = new Scanner(new File(fileName)); 
	List<Point> pointlist =new ArrayList<Point>();
	while (sc.hasNext()) 
	{
		String line = sc.nextLine(); 
		String values [] = line.split(" ");
		
		Point point = new Point(Integer.parseInt(values[0]),Integer.parseInt(values[1]));
		pointlist.add(point);
	System.out.println(line); 
	} sc.close();
	
	
	points = pointlist.toArray(new Point[pointlist.size()]);
	
	 } 
	 catch (IOException e) 
	 { // TODO Auto-generated catch block e.printStackTrace(); 
	 } 
	return points;
	
	 }

}


