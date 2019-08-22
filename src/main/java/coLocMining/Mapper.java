package coLocMining;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.awt.Point;
import java.util.*;
import java.util.concurrent.atomic.LongAccumulator;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrameReader;
import scala.Tuple2;
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

        //count of number of instances of each event type

		JavaRDD<Object> allSpatialObjects = lines.map(x -> create_Object(x));
//		HashMap<String,Integer> countNumOfInst=new HashMap<>();
//		for(Object obj:allSpatialObjects.collect())
//		{
//			if(countNumOfInst.containsKey(obj.event_type))
//			{
//				int prev=countNumOfInst.get(obj.event_type);
//				countNumOfInst.replace(obj.event_type,prev+1);
//			}
//			else
//			{
//				countNumOfInst.put(obj.event_type,1);
//			}
//		}

		JavaPairRDD<String,Integer> countNumOfInst=allSpatialObjects
				.flatMap(object -> Arrays.asList(object.event_type)
						.iterator()).mapToPair(event -> new Tuple2<>(event,1)).reduceByKey((a,b)->a+b);

		System.out.println("Count of num of instance of each type");
		for(Tuple2 t:countNumOfInst.collect())
		{
			System.out.println(t._1+" "+t._2);
		}
		//mapping objects to grid number

		JavaPairRDD<Object,GridNo> allGridValues=lines.mapToPair((PairFunction<String, Object, GridNo>) s -> {
			Object o=create_Object(s);
			return new Tuple2<Object, GridNo>(o,findRegion(o,0.5));
		});

		PrintWriter writer = new PrintWriter("Grid_values.txt", "UTF-8");
		
	     	
		
		for (Tuple2 value: allGridValues.collect()) {
			Object o= (Object) value._1;
			GridNo gridNo= (GridNo) value._2;
			writer.println(o.event_type+" "+o.instance_id+" "+o.x+" "+o.y+" => "+gridNo.grid_x+" "+gridNo.grid_y);
			
			
		}
		
		
		writer.close();

		JavaPairRDD<Object, List<Object>> starNeighbour=PlaneSweep.closestPair(allGridValues,0.5);
		writer = new PrintWriter("Star_Neighbour.txt", "UTF-8");
		for (Tuple2 value: starNeighbour.collect()) {
			Object o= (Object) value._1;
			List<Object> list= (LinkedList<Object>) value._2;
			writer.print(o.event_type+" "+o.instance_id+" => ");
			for(Object obj :list)
			{
				writer.print(obj.event_type+" "+obj.instance_id+" , ");
			}
			writer.println();


		}
		writer.close();
//		Point[] pts = Read_Points.readTex=tFileUsingScanner("Grid_values.txt");
//		Point[] pairs = PlaneSweep.closestPair(pts);
//		PrintWriter writer1 = new PrintWriter("Closest_Pairs.txt", "UTF-8");
//		for ( int i=0; i<pairs.length;i++ )
//		writer1.println(pairs[i].x+" "+pairs[i].y);
//
//		writer1.close();
		
		sc.stop();
	}

}

