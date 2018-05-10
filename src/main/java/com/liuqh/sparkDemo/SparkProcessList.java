package com.liuqh.sparkDemo;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SparkProcessList {
	public static void main(String[] args) {
		 //设置应用程序的名称和运行模式(本地)  
        SparkConf conf = new SparkConf()  
                .setAppName("Spark WordCount by Java.").setMaster("local[*]");  
          
        //创建Java SparkContext,  
        //通往天堂之门（去集群的唯一通道）  
        JavaSparkContext sc = new JavaSparkContext(conf); 
        List<String> wordsList=new ArrayList<String>();
        wordsList.add("abc 123");
        wordsList.add("ab 1234 321");
        wordsList.add("abd 123");
        wordsList.add("ddd 123 ab");
       JavaRDD<String> wordsRDD=  sc.parallelize(wordsList);
       
     //对每行进行拆分，  
       JavaRDD<String> words = wordsRDD.flatMap(new FlatMapFunction<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 2545178394333070978L;

			@Override
			public Iterator<String> call(String t) throws Exception {
				System.out.println("aaa");
				List<String> list=Arrays.asList(t.split(" "));
				return list.iterator();
			}

			
       }).repartition(4).cache();  
       
       //对单词实例进行计数为1  
       JavaPairRDD<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {  
           /**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override  
           public Tuple2<String, Integer> call(String word) throws Exception { 
				System.out.println("bbb");
               return new Tuple2<String,Integer>(word,1) ;  
           }  
       });  
         
       // 统计每个单词在文件中出现的总次数  
       JavaPairRDD<String,Integer> wordsCount =   
               pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {   
               /**
					 * 
					 */
					private static final long serialVersionUID = 1L;

			// 对相同的key，对value进行累加，可以local和reducer级别同时reduce，提高网络带宽利用率  
           @Override  
           public Integer call(Integer v1, Integer v2) throws Exception {  
           	System.out.println("ccc");
               return v1 + v2;  
           }  
       });  
       wordsCount.repartition(1).foreach(new VoidFunction<Tuple2<String,Integer>>() {  
           /**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override  
           public void call(Tuple2<String, Integer> pairs) throws Exception { 
				System.out.println("ddd");
               System.out.println(pairs._1 + ":" + pairs._2);  
           }  
       });   
       //关闭sc上下文  
       sc.close();  
		
	}
}
