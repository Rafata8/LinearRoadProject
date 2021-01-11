package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Int;



import java.util.Iterator;

public class exercise2 {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // get input data
        DataStream<String> text;

        // read the text file from given input path
        text = env.readTextFile(params.get("input"));

        // read the speed limit
        final Integer speed = Integer.parseInt(params.get("speed"));

        final Integer time = Integer.parseInt(params.get("time"));
        //String time = params.get("time");

        // read the speed limit
        final Integer startSegment = Integer.parseInt(params.get("startSegment"));
        //String startSegment = params.get("startSegment");
        // read the speed limit
        final Integer endSegment = Integer.parseInt(params.get("endSegment"));
        //String endSegment = params.get("endSegment");


        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

/*      0=time  f0
        1=vid   f1
        2=spd   f2
        3=xway  f3
        5=dir   f4
        6=seg   f5
        */

        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> mapStream = text.
                map(new MapFunction<String, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> map(String in) throws Exception{
                        String[] fieldArray = in.split(",");
                        Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> out = new Tuple6(Integer.parseInt(fieldArray[0]),
                                Integer.parseInt(fieldArray[1]), Integer.parseInt(fieldArray[2]), Integer.parseInt(fieldArray[3]),Integer.parseInt(fieldArray[5]),Integer.parseInt(fieldArray[6]));
                        return out;
                    }
                }) .filter(new FilterFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
            public boolean filter(Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                boolean isInSegment=startSegment<=in.f5 && endSegment>=in.f5;
                boolean isEastbound=in.f4.equals(0);
                boolean hasSpeed=in.f2>speed;

                return (isEastbound && isInSegment && hasSpeed);
            }
        })
                ;



        KeyedStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Tuple> keyedStream = mapStream.
                assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> element) {
                                return element.f0*1000; ///ahi habia un *1000
                            }
                        }

                ).keyBy(3);


        SingleOutputStreamOperator<Tuple4<Integer, Integer, Integer,String>> sumTumblingEventTimeWindows =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds((long) time))).apply(new SimpleSum());




        // emit result
        if (params.has("output")) {
            //sumTumblingEventTimeWindows.writeAsText(params.get("output"));
            sumTumblingEventTimeWindows.writeAsCsv(params.get("output"));
        }

        // execute program
        env.execute("exercise2");
    }

    public static class SimpleSum implements WindowFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Tuple4<Integer, Integer, Integer,String>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> input, Collector<Tuple4<Integer, Integer, Integer,String>> out) throws Exception {
            Iterator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
            Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();
            Integer xway = 0;
            Integer ts = 0;
            Integer quantity=0;
            String vids="[ ";
            if(first!=null){
                xway = first.f3;
                ts = first.f0;
                quantity=1;
                vids+=String.valueOf(first.f1);
            }
            while(iterator.hasNext()){
                Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> next = iterator.next();
                //ts = next.f0;
                //xexit += next.f2;
                quantity+=1;
                vids+=" - "+String.valueOf(next.f1);
            }
            out.collect(new Tuple4<Integer, Integer, Integer,String>(ts, xway, quantity,vids+" ]"));
        }
    }







}