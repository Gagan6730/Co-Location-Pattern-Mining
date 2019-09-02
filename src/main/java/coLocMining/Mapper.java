package coLocMining;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.util.*;

import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.hadoop.mapreduce.Mapper;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
//import org.apache.spark.Logging;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper {

    public static Double ParticipationIndex(LinkedList<String> candidate_colocation, LinkedList<LinkedList<Object>> instances, JavaPairRDD<String,Integer> countNumOfInst)
    {
        //creating a map from event type to integer to store the number of instances of each event type
        HashMap<String,Integer> countMap=new HashMap<>();
        for(Tuple2<String,Integer> t:countNumOfInst.collect())
        {
            countMap.put(t._1,t._2);
        }

        //java rdd for all instances. Rather that grouping the according to co-location event types, this is just a list of all
        //instances. Eg. A1,B1,C1,A1,B1,C2,A2,B2,C2
        HashMap<String, HashSet<Object>> objectSet=new HashMap<>();
        for(LinkedList<Object> list: instances)
        {
            for(Object object:list)
            {
                if(objectSet.containsKey(object.event_type))
                {
                    HashSet<Object> set=objectSet.get(object.event_type);
                    set.add(object);
                    objectSet.replace(object.event_type,set);
                }
                else
                {
                    HashSet<Object> set=new HashSet<>();
                    set.add(object);
                    objectSet.put(object.event_type,set);
                }
            }
        }

        Double PI=Double.MAX_VALUE;
        for(String str:candidate_colocation)
        {
            Double participationRatio=(objectSet.get(str).size()/(double)countMap.get(str));
            if(participationRatio<PI)
            {
                PI=participationRatio;
            }
        }



        return PI;
    }
    private static LinkedList<LinkedList<String>> GenerateCandidateColocations(JavaPairRDD<String,Integer> countNumOfInst,int k)
    {
        LinkedList<String> input=new LinkedList<>();
        for(Tuple2<String, Integer> t:countNumOfInst.collect())
        {
            input.add(t._1);
        }
        Collections.sort(input);
        LinkedList<LinkedList<String>> candidate=new LinkedList<>();
        int opsize= (int) Math.pow(2,input.size());
        for(int c=1;c<opsize;c++)
        {
            LinkedList<String> list=new LinkedList<>();
            for(int j=0;j<input.size();j++)
            {
                if(BigInteger.valueOf(c).testBit(j))
                {
                    list.add(input.get(j));
                }
            }
            if(list.size()==k)
            {
                candidate.add(list);
            }
        }
//
//            LinkedList<String> list=new LinkedList<>();
//            int finalC = c;
//            JavaPairRDD<String,Long> temp=eventTypes.filter(new Function<Tuple2<String, Long>, Boolean>() {
//                @Override
//                public Boolean call(Tuple2<String, Long> stringLongTuple2) throws Exception {
//                    if(BigInteger.valueOf(finalC).testBit(Math.toIntExact(stringLongTuple2._2)))
//                    {
//                        return true;
//                    }
//                    else
//                    {
//                        return false;
//                    }
//                }
//            });
//            if(temp.count()==k)
//            {
//                for(Tuple2<String,Long> t:eventTypes.collect())
//                {
//                    list.add(t._1);
//                }
//                candidate.add(list);
//            }
//
//        }
        return candidate;
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

	/*
	creating subsets of size k-1 for colocation of size k
	 */
	public static LinkedList<LinkedList<String>> generateSubsets(LinkedList<String> list,int k)
    {
        LinkedList<LinkedList<String>> subsets=new LinkedList<>();
        int opsize= (int) Math.pow(2,list.size());
        for(int c=1;c<opsize;c++)
        {
            LinkedList<String> temp_list=new LinkedList<>();
            for(int j = 0; j< list.size(); j++)
            {
                if(BigInteger.valueOf(c).testBit(j))
                {
                    list.add(list.get(j));
                }
            }
            if(list.size()==k)
            {
                subsets.add(list);
            }
        }

        return subsets;
	}

	/*
	checks if all subsets ,of size k-1 ,of colocation of size k exist in colocation of size k-1
	 */
	public static boolean checkSubsets(LinkedList<LinkedList<String>> prevColocations, LinkedList<String> candidates)
    {
        Collections.sort(candidates);

        int f=0;
        for(LinkedList<String> list:prevColocations)
        {

            Collections.sort(list);
            if(list.size()!=candidates.size())
            {
                continue;
            }
            else
            {
                int flag=0;
                for (int i = 0; i <list.size() ; i++) {
                    if(!candidates.get(i).equals(list.get(i)))
                    {
                        flag=1;
                        break;
                    }

                }
                if(flag==0)
                {
                    return true;
                }
            }
        }
        return false;
    }

    public static LinkedList<LinkedList<Object>> mergeInstancesFirst(HashSet<Object> first, HashSet<Object> second)
    {
        LinkedList<LinkedList<Object>> instancePattern=new LinkedList<>();
        for(Object obj1:first)
        {
            LinkedList<Object> list=new LinkedList<>();
            list.add(obj1);
            for(Object obj2:second)
            {
                list.add(obj2);
                instancePattern.add(list);
                list=new LinkedList<>();
            }
        }
        return instancePattern;
    }
    public static LinkedList<LinkedList<Object>> mergeInstances(LinkedList<LinkedList<Object>> instancePattern, HashSet<Object> second)
    {
        LinkedList<LinkedList<Object>> returnList=new LinkedList<>();
        for(LinkedList<Object> list:instancePattern)
        {
            LinkedList<Object> tempList = new LinkedList<>(list);
            for(Object obj:second)
            {
               tempList.add(obj);
               returnList.add(tempList);
               tempList = new LinkedList<>(list);
            }
        }
        return returnList;
    }

    public static LinkedList<LinkedList<Object>> createInstancesOfSizeK(HashMap<String,HashSet<Object>> objectOfEachType)
    {
        LinkedList<HashSet<Object>> objectList=new LinkedList<>();
        for(Map.Entry<String,HashSet<Object>> m:objectOfEachType.entrySet())
        {
            objectList.add(m.getValue());
        }
        LinkedList<LinkedList<Object>> finalInstancePattern=new LinkedList<>();
        for(int i=1;i<objectList.size();i++)
        {
            if(i==1)
            {
                finalInstancePattern=mergeInstancesFirst(objectList.get(0),objectList.get(1));
            }
            else
            {
                LinkedList<LinkedList<Object>> tempList=new LinkedList<>(finalInstancePattern);
                finalInstancePattern=new LinkedList<>(mergeInstances(tempList,objectList.get(i)));
            }
        }
        return finalInstancePattern;
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

		final long numberOfFeatures=countNumOfInst.count();
		final Double threshPI=0.3;
		System.out.println("Count of num of instance of each type");
		for(Tuple2<String, Integer> t:countNumOfInst.collect())
		{
			System.out.println(t._1+" "+t._2);
		}
		//mapping objects to grid number

		JavaPairRDD<Object,GridNo> allGridValues=lines.mapToPair((PairFunction<String, Object, GridNo>) s -> {
			Object o=create_Object(s);
			return new Tuple2<Object, GridNo>(o,findRegion(o,0.5));
		});

		PrintWriter writer = new PrintWriter("Grid_values.txt", "UTF-8");
		
	     	
		
		for (Tuple2<Object, GridNo> value: allGridValues.collect()) {
			Object o= value._1;
			GridNo gridNo= value._2;
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

//        JavaRDD<LinkedList<String>> co_location_patterns;
        HashMap<Integer,LinkedList<LinkedList<String>>> co_location_patterns=new HashMap<>();
        JavaPairRDD<LinkedList<String>,LinkedList<LinkedList<Object>>> instancesOfSizeK;
        int k=2;
        while(k<=numberOfFeatures)
        {
            //candidate co-locations of size k
            JavaRDD<LinkedList<String>> candidateColocations=sc.parallelize(GenerateCandidateColocations(countNumOfInst,k));
            System.out.println("Candidate Colocations");
            for(LinkedList<String> list:candidateColocations.collect())
            {
                for(String s:list)
                {
                    System.out.print(s+" ");
                }
                System.out.println();
            }
            if(k==2)
            {
                List<Tuple2<Object, List<Object>>> starNeighList=starNeighbour.collect();

                //creating instances of size 2
                 instancesOfSizeK=candidateColocations.mapToPair(new PairFunction<LinkedList<String>, LinkedList<String>, LinkedList<LinkedList<Object>>>() {
                    @Override
                    public Tuple2<LinkedList<String>, LinkedList<LinkedList<Object>>> call(LinkedList<String> colocation) throws Exception {
                        String EventType1=colocation.get(0);
                        LinkedList<LinkedList<Object>> finalList=new LinkedList<>();
                        for(Tuple2<Object, List<Object>> t:starNeighList)
                        {
                            if(t._1.event_type.equals(EventType1)) {
                                LinkedList<Object> tempList = new LinkedList<>();
                                for (int i = 0; i < t._2.size(); i++) {
                                    if (t._2.get(i).event_type.equals(colocation.get(1))) {
                                        tempList.add(t._2.get(i));
                                    }
                                }
                                for (int i = 0; i < tempList.size(); i++) {
                                    LinkedList<Object> list = new LinkedList<>();
                                    list.add(t._1);
                                    list.add(tempList.get(i));
                                    finalList.add(list);
                                }
                            }


                        }

//                        JavaRDD<LinkedList<Object>> finalInstancesForColocation=sc.parallelize(finalList);

                        return new Tuple2<>(colocation,finalList);
                    }
                });


                for(Tuple2<LinkedList<String>,LinkedList<LinkedList<Object>>> tuple2:instancesOfSizeK.collect())
                {
                    Double PI=ParticipationIndex(tuple2._1,tuple2._2,countNumOfInst);
                    LinkedList<String> list=tuple2._1;
                    System.out.print("(");
                    for(int i=0;i<list.size()-1;i++)
                    {
                        System.out.print(list.get(i)+",");
                    }
                    System.out.println(list.get(list.size()-1)+") => "+PI);


                    LinkedList<LinkedList<Object>> rdd=tuple2._2;
                    for(LinkedList<Object> l:rdd)
                    {
                        System.out.print("\t");
                        for(int i=0;i<l.size();i++)
                        {
                            System.out.print(l.get(i).event_type+l.get(i).instance_id+" ");
                        }

                        System.out.println();
                    }
                }

//                Double PI=Double.MAX_VALUE;
                LinkedList<LinkedList<String>> coloc=new LinkedList<>();
                for(Tuple2<LinkedList<String>,LinkedList<LinkedList<Object>>> tuple2:instancesOfSizeK.collect())
                {
                    Double PI=ParticipationIndex(tuple2._1,tuple2._2,countNumOfInst);

                    if(PI>=threshPI)
                    {
                        coloc.add(tuple2._1);
                    }
                }
                JavaRDD<LinkedList<String>> co_location=sc.parallelize(coloc);
                writer = new PrintWriter("CoLocation_Pattern.txt", "UTF-8");
                for (LinkedList<String> list: co_location.collect()) {

                    for(String obj :list)
                    {
                        writer.print(obj+" ");
                    }
                    writer.println();


                }

                LinkedList<LinkedList<String>> final_colocation = new LinkedList<>(co_location.collect());
                co_location_patterns.put(k,final_colocation);
//                writer.close();

//                instancesOfSizeK.filter(new Function<Tuple2<LinkedList<String>, LinkedList<LinkedList<Object>>>, Boolean>() {
//                    @Override
//                    public Boolean call(Tuple2<LinkedList<String>, LinkedList<LinkedList<Object>>> pair) throws Exception {
//                        JavaRDD<String> candidate=sc.parallelize(pair._1)
//                        return null;
//                    }
//                })
//                instancesOfSizeK_1=instancesOfSizeK;
            }
            else
            {
                int subsetSize=k-1;
                //checking if all subsets of a k size colocation exists in k-1 size colocation
                JavaRDD<LinkedList<String>> allColocationsK= candidateColocations.filter(new Function<LinkedList<String>, Boolean>() {
                    @Override
                    public Boolean call(LinkedList<String> strings) throws Exception {
                        LinkedList<LinkedList<String>> subset_of_size_k=generateSubsets(strings,subsetSize);
                        int flag=0;
                        for(LinkedList<String> list:subset_of_size_k)
                        {
                            if(!checkSubsets(co_location_patterns.get(subsetSize),list))
                            {
                                flag=1;
                            }
                        }
                        if(flag==0)
                        {
//                            candidates_of_size_k.add(strings);
                            return true;
                        }
                        else
                        {
                            return false;
                        }
                    }
                });

                HashMap<LinkedList<String>,LinkedList<LinkedList<Object>>> instancesOfSizeKMap=new HashMap<>();
                //creating instances for each colocation of size k
                for(LinkedList<String> list:allColocationsK.collect())
                {
                    Collections.sort(list);

                    /*
                eg: colocation: BCD
                B-> b1,b2
                C-> c1,c3
                d->d2
                 */
                    LinkedList<LinkedList<Object>> InstanceForColocationK=new LinkedList<>();


                    for(Tuple2<Object,List<Object>> tuple2:starNeighbour.collect())
                    {
                        HashMap<String, HashSet<Object>> objectOfEachType=new HashMap<>();
                        for(String s:list)
                        {
                            objectOfEachType.put(s,new HashSet<>());
                        }
                        if(tuple2._1.event_type.equals(list.getFirst()))
                        {

                            HashSet<Object> set=objectOfEachType.get(tuple2._1.event_type);
                            set.add(tuple2._1);
                            objectOfEachType.replace(tuple2._1.event_type,set);
                            for(Object o:tuple2._2)
                            {
                                if(list.contains(o.event_type))
                                {
                                    set=objectOfEachType.get(o.event_type);
                                    set.add(o);
                                    objectOfEachType.replace(o.event_type,set);
                                }
                            }
                        }
                        int flag=0;
                        for(Map.Entry<String, HashSet<Object>> m:objectOfEachType.entrySet())
                        {
                            HashSet<Object> set= m.getValue();
                            if(set.size()==0)
                            {
                                flag=1;
                                break;
                            }
                        }
                        if(flag==0)
                        {
                            LinkedList<LinkedList<Object>> InstancesForOneRowInstance= createInstancesOfSizeK(objectOfEachType);
                            InstanceForColocationK.addAll(InstancesForOneRowInstance);
                        }


                    }
                    instancesOfSizeKMap.put(list,InstanceForColocationK);

                }

                /*
                after checking if all instance of colocation of size k
                remove the lowest event type alphabetically
                then check if there exist an instance in prev instances

                 */
                HashMap<LinkedList<String>,LinkedList<LinkedList<Object>>> FinalInstancesOfSizeKMap=new HashMap<>();
                for(Map.Entry<LinkedList<String>,LinkedList<LinkedList<Object>>> m:instancesOfSizeKMap.entrySet())
                {
                    
                }

            }
            k++;
        }

		sc.stop();
	}

}

