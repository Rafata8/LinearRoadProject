package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
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

public class exercise1 {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // get input data
        DataStream<String> text;

        // read the text file from given input path
        text = env.readTextFile(params.get("input"));


        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> mapStream = text.
                map(new MapFunction<String, Tuple3<Integer, Integer, Integer>>() {
                    public Tuple3<Integer, Integer, Integer> map(String in) throws Exception{
                        String[] fieldArray = in.split(",");
                        Tuple3<Integer,Integer,Integer> out = new Tuple3(Integer.parseInt(fieldArray[0]),
                                Integer.parseInt(fieldArray[3]), Integer.parseInt(fieldArray[4]));
                        return out;
                    }
                }) .filter(new FilterFunction<Tuple3<Integer, Integer, Integer>>() {
                        public boolean filter(Tuple3<Integer, Integer, Integer> in) throws Exception {
                            return in.f2.equals(4);
                        }
                    })
                ;



        KeyedStream<Tuple3<Integer, Integer, Integer>, Tuple> keyedStream = mapStream.
                assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple3<Integer, Integer, Integer>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple3<Integer, Integer, Integer> element) {
                                return element.f0*1000; ///ahi habia un *1000
                            }
                        }

                ).keyBy(1);


        SingleOutputStreamOperator<Tuple4<Integer, Integer, Integer,Integer>> sumTumblingEventTimeWindows =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds((long) 3600))).apply(new SimpleSum());





        // emit result
        if (params.has("output")) {
            sumTumblingEventTimeWindows.writeAsCsv(params.get("output"));
            //sumTumblingEventTimeWindows.writeAsCsv("..\\examples-output\\pruebaaaaa.csv");
        }

        // execute program
        env.execute("exercise1");
    }

    public static class SimpleSum implements WindowFunction<Tuple3<Integer, Integer, Integer>, Tuple4<Integer, Integer, Integer,Integer>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<Integer, Integer, Integer>> input, Collector<Tuple4<Integer, Integer, Integer,Integer>> out) throws Exception {
            Iterator<Tuple3<Integer, Integer, Integer>> iterator = input.iterator();
            Tuple3<Integer, Integer, Integer> first = iterator.next();
            Integer xway = 0;
            Integer ts = 0;
            Integer xexit = 0;
            Integer quantity=0;
            if(first!=null){
                xway = first.f1;
                ts = first.f0;
                xexit = first.f2;
                quantity=1;
            }
            while(iterator.hasNext()){
                Tuple3<Integer, Integer, Integer> next = iterator.next();
                //ts = next.f0;
                //xexit += next.f2;
                quantity+=1;
            }
            out.collect(new Tuple4<Integer, Integer, Integer,Integer>(ts, xway, xexit,quantity));
        }
    }







}