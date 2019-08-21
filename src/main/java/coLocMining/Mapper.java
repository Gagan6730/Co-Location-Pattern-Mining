package coLocMining;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.awt.Point;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrameReader;
//import org.apache.spark.Logging;

public class Mapper {

	public static Object create_Object(String line)
	{
		String values [] = line.split(" ");
		Object o = new Object(values[0],Integer.parseInt(values[1]), Double.parseDouble(values[2]), Double.parseDouble(values[3]) );
		return o;
	}
	public static GridNo findRegion(Object o, double minDist)
	{ 
		GridNo gn = new GridNo();
		double x = java.lang.Math.floor(o.getX()/minDist);
		double y = java.lang.Math.floor(o.getY()/minDist);
		gn.setGrid_x((int)x);
		gn.setGrid_y((int)y);
		return gn;
		
	}
	
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
		
		SparkConf sf = new SparkConf().setMaster("local[3]").setAppName("GetRegion");
        JavaSparkContext sc = new JavaSparkContext(sf);
        JavaRDD<String> lines = sc.textFile("data.txt");
        
		/*
		 * SparkSession spark = SparkSession .builder() .appName("GetRegion")
		 * .getOrCreate(); JavaRDD<String> lines =
		 * spark.read().textFile(args[0]).javaRDD();
		 */
		JavaRDD<Object> allSpatialObjects = lines.map(x -> create_Object(x));
		JavaRDD<GridNo> allGridValues = allSpatialObjects.map(x -> findRegion(x,0.5)) ;
		
		System.out.println(allGridValues.collect());
		
		PrintWriter writer = new PrintWriter("Grid_values.txt", "UTF-8");
		
	     	
		
		for (GridNo value: allGridValues.collect()) {
			writer.println(value.getGrid_x()+" "+value.getGrid_y());
			
			
		}
		
		
		writer.close();
		
		Point[] pts = Read_Points.readTextFileUsingScanner("Grid_values.txt");
		Point[] pairs = PlaneSweep.closestPair(pts);
		PrintWriter writer1 = new PrintWriter("Closest_Pairs.txt", "UTF-8");
		for ( int i=0; i<pairs.length;i++ )
		writer1.println(pairs[i].x+" "+pairs[i].y);

		writer1.close();
		
		sc.stop();
	}

}

