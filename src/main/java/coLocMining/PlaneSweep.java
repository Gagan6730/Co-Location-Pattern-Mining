//https://baptiste-wicht.com/posts/2010/04/closest-pair-of-point-plane-sweep-algorithm.html

package coLocMining;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.awt.Point;
import java.util.*;

public class PlaneSweep {
    private static double calca_dist(Point s1,Point s2)
    {
        double diff_x=Math.pow(s1.getX()-s2.getX(),2);
        double diff_y=Math.pow(s1.getY()-s2.getY(),2);

        return Math.sqrt(diff_x+diff_y);
    }

public static JavaPairRDD<Object, List<Object>> closestPair(JavaPairRDD<Object,GridNo> allGridValues,double thresh_dist) {
    Point[] closestPair = new Point[2];

    //When we start the min distance is the infinity
    double crtMinDist = thresh_dist;

    JavaPairRDD<Object,List<Object>> starNeighbour=allGridValues.mapToPair(new PairFunction<Tuple2<Object, GridNo>, Object, List<Object>>() {
        @Override
        public Tuple2<Object, List<Object>> call(Tuple2<Object, GridNo> input) throws Exception {

            //getting all objects
            JavaRDD<Object> objectList=allGridValues.map(new Function<Tuple2<Object, GridNo>, Object>() {
                @Override
                public Object call(Tuple2<Object, GridNo> objectGridNoTuple2) throws Exception {

                    return objectGridNoTuple2._1;
                }
            });
            LinkedList<Object> activeSet=new LinkedList<>();
            for(Object obj: objectList.collect())
            {
                if(Math.abs(input._1.x-obj.x)>crtMinDist)
                {
                    activeSet.remove(obj);
                }
            }


            return null;
        }
    })
//            flatMapToPair(new PairFlatMapFunction<Tuple2<Object, GridNo>, Object, List<Object>>() {
//        @Override
//        public Iterator<Tuple2<Object, List<Object>>> call(Tuple2<Object, GridNo> objectGridNoTuple2) throws Exception {
//            return null;
//        }
//    })

//    //Get the points and sort them
//    Point[] ObjectSet = Arrays.copyOf(points, points.length);
//    Arrays.sort(ObjectSet, HORIZONTAL_COMPARATOR);
//    System.out.println("ObjectSet" + " Points");
//    for(Point p:ObjectSet)
//    {
//        System.out.println(p.x+" "+p.y);
//    }
//
////    //When we start the left most candidate is the first one
////    int leftMostCandidateIndex = 0;
//
//    int index=0;
//    //Vertically ObjectSet
//    // set of candidates
////    ObjectSet
////    Set<Point> candidates = new TreeSet<>(VERTICAL_COMPARATOR);
//
//    Set<Point> ActiveSet=new TreeSet<>(VERTICAL_COMPARATOR);
//    System.out.println(ActiveSet.size());
//    //For each point from left to right
//    for (Point current : ObjectSet) {
//        //Shrink the candidates
//        while (Math.abs(current.x - ObjectSet[index].x) > crtMinDist) {
//            ActiveSet.remove(ObjectSet[index]);
//            index++;
//        }
//
//        LinkedList<Point> RangeOnY= (LinkedList<Point>) Arrays.asList((Point [])ActiveSet.toArray());
//        Iterator itr=RangeOnY.iterator();
//        while(itr.hasNext())
//        {
//            Point p= (Point) itr.next();
//            if(Math.abs(current.y-p.y)>crtMinDist)
//            {
//                ActiveSet.remove(p);
//            }
//        }
//
//        RangeOnY= (LinkedList<Point>) Arrays.asList((Point [])ActiveSet.toArray());
//        itr=RangeOnY.iterator();
//        while(itr.hasNext())
//        {
//            Point p= (Point) itr.next();
//            if(calca_dist(current,p)<=crtMinDist)
//            {
//                itr.next();
//            }
//        }
//
//        ActiveSet.add(current);
//
//        //Compute the y head and the y tail of the candidates set
////        Point head = new Point(current.x, (int) (current.y - crtMinDist));
////        Point tail = new Point(current.x, (int) (current.y + crtMinDist));
////
////        //We take only the interesting candidates in the y axis
////        for (Point point : ActiveSet.subSet(head, tail)) {
////            double distance = current.distance(point);
////
////            //Simple min computation
////            if (distance < crtMinDist) {
////                crtMinDist = distance;
////
////                closestPair[0] = current;
////                closestPair[1] = point;
////            }
////        }
////
////        //The current point is now a candidate
////        candidates.add(current);
//    }
//
//    return closestPair;
}

private static final Comparator<Point> VERTICAL_COMPARATOR = new Comparator<Point>() {
    @Override
    public int compare(Point a, Point b) {
        if (a.y < b.y) {
            return -1;
        }
        if (a.y > b.y) {
            return 1;
        }
        if (a.x < b.x) {
            return -1;
        }
        if (a.x > b.x) {
            return 1;
        }
        return 0;
    }
};

private static final Comparator<Point> HORIZONTAL_COMPARATOR = new Comparator<Point>() {
    @Override
    public int compare(Point a, Point b) {
        if (a.x < b.x) {
            return -1;
        }
        if (a.x > b.x) {
            return 1;
        }
        if (a.y < b.y) {
            return -1;
        }
        if (a.y > b.y) {
            return 1;
        }
        return 0;
    }
};
}