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
//import org.apache.hadoop.mapreduce.Mapper;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrameReader;
import scala.Tuple2;
//import org.apache.spark.Logging;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper {

    public static Double ParticipationIndex(JavaRDD<String> candidate_colocation, JavaRDD<LinkedList<Object>> instances, JavaPairRDD<String,Integer> countNumOfInst)
    {
        HashMap<String,Integer> countMap=new HashMap<>();
        for(Tuple2<String,Integer> t:countNumOfInst.collect())
        {
            countMap.put(t._1,t._2);
        }

        JavaRDD<Object> allObjectsInInstances=instances.flatMap(new FlatMapFunction<LinkedList<Object>, Object>() {
            @Override
            public Iterator<Object> call(LinkedList<Object> objects) throws Exception {
                Iterator itr=objects.iterator();
                return itr;
            }
        });
        JavaPairRDD<String,Long> distinctCountInInstance=candidate_colocation.mapToPair(new PairFunction<String, String, Long>() {
            @Override
            public Tuple2<String, Long> call(String s) throws Exception {
                long distictCount=allObjectsInInstances.filter(new Function<Object, Boolean>() {
                    @Override
                    public Boolean call(Object object) throws Exception {
                        if(object.event_type.equals(s))
                        {
                            return true;
                        }
                        else
                        {
                            return false;
                        }
                    }
                }).distinct().count();

                return new Tuple2<>(s,distictCount);
            }
        });


        JavaPairRDD<String,Double> participationRatio=distinctCountInInstance.mapToPair(new PairFunction<Tuple2<String, Long>, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                double PR=(stringLongTuple2._2/(double)countMap.get(stringLongTuple2._1));
                return new Tuple2<>(stringLongTuple2._1,PR);
            }
        });

        Double PI=participationRatio.map(new Function<Tuple2<String, Double>, Double>() {
            @Override
            public Double call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return stringDoubleTuple2._2;
            }
        }).min(new Comparator<Double>() {
            @Override
            public int compare(Double o1, Double o2) {
                if(o1<o2)
                {
                    return 1;
                }
                else if(o1==o2)
                {
                    return 0;
                }
                else {
                    return -1;
                }
            }
        });


        return PI;
    }
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

	public static void generateColocations(JavaRDD<String> eventTypes,int k)
	{

	}
	
	public static void main(String[] args) throws FileNotFoundException, IOException  {
		
		SparkConf sf = new SparkConf().setMaster("local[3]").setAppName("GetRegion");
        JavaSparkContext sc = new JavaSparkContext(sf);
        JavaRDD<String> lines = sc.textFile("data.txt");

		JavaRDD<Object> allSpatialObjects = lines.map(x -> create_Object(x));

		//count of number of instances of each event type
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

		//list of star neighbours
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

		JavaRDD<List<String>> candidateColocation;

		sc.stop();
	}

}

