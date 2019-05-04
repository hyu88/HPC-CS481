// Step 1: import required classes and interfaces
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;


import java.util.Arrays;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collections;

public class CountQuadrilaterals {
	public static List<String> out = new ArrayList<String>();
	public static void main(String[] args) throws Exception {
		// Step 2; read and validate input parameters
		if (args.length < 2) {
			System.err.println("Usage: CountTriangles <input> <output>");
			System.exit(1);
		}
		String input = args[0];
		String output = args[1];
		// int size = Integer.parseInt(args[2]);
		// List<String> out = new ArrayList<String>();

		// Step 3: create a JavaSparkContext object
		// This is the object used for creating the first RDD.
		JavaSparkContext ctx = new JavaSparkContext();

		//  Step 4: read an HDFS input text file representing a graph;
		//  records are represented as JavaRDD<String>
		JavaRDD<String> lines = ctx.textFile(input);
		lines.cache();

		// timestamp
		long start = System.currentTimeMillis();

		// Step 5: create a new JavaPairRDD for all edges, which
		// includes (source, destination) and (destination, source)
		// PairFlatMapFunction<T, K, V>
		// T => Iterator<Tuple2<K,V>>
		JavaPairRDD<Long, Long> edges =
			lines.flatMapToPair(new PairFlatMapFunction<String, Long, Long>() {
				@Override
				public Iterator<Tuple2<Long, Long>> call(String s) {
					String[] nodes = s.split(" ");
					// long start = Long.parseLong(nodes[0]);
					// long end = Long.parseLong(nodes[1]);
					long start = Long.parseLong(nodes[0]);
					long end = Long.parseLong(nodes[1]);
					return Arrays.asList(new Tuple2<Long, Long>(start, end),
					                     new Tuple2<Long, Long>(end, start)).iterator();
			}
		});

		// Step 6: create a new JavaPairRDD, which will generate triads
		JavaPairRDD<Long, Iterable<Long>> triads = edges.groupByKey();

		/*
		// Step 6.1: debug1
		out.add("=== Output log for debug1 ===");
		List<Tuple2<Long,  Iterable<Long>>> debug1 = triads.collect();
		for (Tuple2<Long, Iterable<Long>> t2: debug1) {
			out.add("debug1 t2._1="+t2._1);
			out.add("debug1 t2._2="+t2._2);
		}
		*/

		// Step 7: create a new JavaPairRDD, which will generate possible triads
		JavaPairRDD<Tuple2<Long, Long>, Long> possibleTriads =
			triads.flatMapToPair(new PairFlatMapFunction<
									Tuple2<Long, Iterable<Long>>, // input
									Tuple2<Long, Long>,           // key (output)
									Long                         // value (output)
									>() {
			@Override
			public Iterator<Tuple2<Tuple2<Long, Long>, Long>> call(Tuple2<Long, Iterable<Long>> s) {

				// s._1 = Long (as a key)
				// s._2 = Iterable<Long> (as values)
				Iterable<Long> values = s._2;
				// we assume that no node has an ID of zero
				List<Tuple2<Tuple2<Long, Long>, Long>>  result =
					new ArrayList<Tuple2<Tuple2<Long, Long>, Long>>();

				// // generate possible possibleTriads
				// for (Long value : values) {
				// 	Tuple2<Long, Long> k2 = new Tuple2<Long, Long>(s._1, value);
				// 	Tuple2<Tuple2<Long, Long>, Long> k2v2 =
				// 		new Tuple2<Tuple2<Long, Long>, Long>(k2, 0l);
				// 	result.add(k2v2);
				// }

				// RDDs values are immutable, so we have to copy the values;
				// copy values to valuesCopy
				List<Long> valuesCopy = new ArrayList<Long>();
				for (Long item : values) {
					valuesCopy.add(item);
				}
				Collections.sort(valuesCopy);

				// generate possible triads
				for (int i = 0; i < valuesCopy.size() - 1; i++) {
					for (int j = i + 1; j < valuesCopy.size(); j++) {
						Tuple2<Long, Long> k2 =
							new Tuple2<Long, Long>(valuesCopy.get(i), valuesCopy.get(j));
						Tuple2<Tuple2<Long, Long>, Long> k2v2 =
							new Tuple2<Tuple2<Long, Long>, Long>(k2, s._1);
						result.add(k2v2);
					}
				}
				return result.iterator();
			}
		});

		/*
		// Step 7.1: debug2
		out.add("=== Output log for debug2 ===");
		List<Tuple2<Tuple2<Long, Long>, Long>> debug2 = possibleTriads.collect();
		for (Tuple2<Tuple2<Long, Long>, Long> t2: debug2) {
			out.add("debug2 t2._1="+t2._1);
			out.add("debug2 t2._2="+t2._2);
		}
		*/

		// Step 8: create a new JavaRDD, which whill generate triangles
		JavaPairRDD<Tuple2<Long, Long>, Iterable<Long>> triadsGrouped =
			possibleTriads.groupByKey();

		/*
		// Step 8.1: debug3
		out.add("=== Output log for debug3 ===");
		List<Tuple2<Tuple2<Long, Long>, Iterable<Long>>> debug3 = triadsGrouped.collect();
		for (Tuple2<Tuple2<Long, Long>, Iterable<Long>> t2: debug3) {
			out.add("debug3 t2._1="+t2._1);
			out.add("debug3 t2._2="+t2._2);
		}
		*/

		// Step 9: create a new JavaPairRDD, which will generate all quadrilaterals
		JavaRDD<Tuple4<Long, Long, Long, Long>> quadrilateralsWithDuplicates =
			triadsGrouped.flatMap(new FlatMapFunction<
							Tuple2<Tuple2<Long, Long>, Iterable<Long>>, // input
							Tuple4<Long, Long, Long, Long>              // output
							>() {
			public Iterator<Tuple4<Long, Long, Long, Long>> call(Tuple2<Tuple2<Long, Long>, Iterable<Long>> s) {

				// s._1 = Tuple2<Long, Long> (as a key) = "<nodeA>,<nodeB>"
				// s._2 = Iterator<Long> (as values) = {0, n1, n2, n3, ...} or {n1, n2, n3, ...}
				Tuple2<Long, Long> key = s._1;
				Iterable<Long> values = s._2;

				List<Long> list = new ArrayList<Long>();
				for (Long node : values)
						list.add(node);

				List<Tuple4<Long, Long, Long, Long>> result =
					new ArrayList<Tuple4<Long, Long, Long, Long>>();
				if (list.size() < 2) {
					// no triangles found
					return result.iterator();
				}

				for (int i = 0; i < list.size(); i++) {
					for (int j = i + 1; j < list.size(); j++) {
						long[] quadri = {key._1, key._2, list.get(i), list.get(j)};
						Arrays.sort(quadri);
						Tuple4<Long, Long, Long, Long> t4 =
							new Tuple4<Long, Long, Long, Long>(quadri[0], quadri[1], quadri[2], quadri[3]);
						result.add(t4);
					}
				}
				return result.iterator();
			}
		});

		/*
		// Step 9.1: debug4
		// print all triangles (includes duplicates)
		out.add("=== Triangles with Duplicates ===");
		List<Tuple3<Long, Long, Long>> debug4 = trianglesWithDuplicates.collect();
		for (Tuple3<Long, Long, Long> t3 : debug4) {
			out.add("t3="+t3);
		}
		*/

		// Step 10: eliminate duplicate triangles and create unique triangles
		JavaRDD<Tuple4<Long, Long, Long, Long>> uniqueQuadrilaterals =
			quadrilateralsWithDuplicates.distinct();

		// Step 10.1: print unique triangles
		out.add("=== Unique Quadrilaterals ===");
		List<Tuple4<Long, Long, Long, Long>> finals = uniqueQuadrilaterals.collect();
		for (Tuple4<Long, Long, Long, Long> t4 : finals) {
			out.add(t4._1() + "," + t4._2() + "," + t4._3() + "," + t4._4());
		}

		// timestamp
		long end = System.currentTimeMillis();
		out.add("quadrilateral num: " + uniqueQuadrilaterals.count());
		out.add("time cost: " + Long.toString(end - start) + "ms");

		// output log
		JavaRDD<String> outString = ctx.parallelize(out, 1);
		outString.saveAsTextFile(output);
	}
}
