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

public class exercise3 {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // get input data
        DataStream<String> text;

        // read the text file from given input path
        text = env.readTextFile(params.get("input"));

        final Integer segment = Integer.parseInt(params.get("segment"));


        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

/*      0=time  f0
        1=vid   f1
        2=spd   f2
        3=xway  f3
        5=dir
        6=seg   f4
        */

        SingleOutputStreamOperator<Tuple5<Integer, Integer, Integer, Integer, Integer>> mapStream = text.
                map(new MapFunction<String, Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
                    public Tuple5<Integer, Integer, Integer, Integer, Integer> map(String in) throws Exception{
                        String[] fieldArray = in.split(",");
                        Tuple5<Integer, Integer, Integer, Integer, Integer> out = new Tuple5(Integer.parseInt(fieldArray[0]),
                                Integer.parseInt(fieldArray[1]), Integer.parseInt(fieldArray[2]), Integer.parseInt(fieldArray[3]), Integer.parseInt(fieldArray[6]));
                        return out;
                    }
                }) .filter(new FilterFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
            public boolean filter(Tuple5<Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                return in.f4.equals(segment);
            }
        })
                ;



        KeyedStream<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple> keyedStream = mapStream.
                assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple5<Integer, Integer, Integer, Integer, Integer> element) {
                                return element.f0*1000; ///ahi habia un *1000
                            }
                        }

                ).keyBy(1);



        //output2, agrupamos porxway y calculamos la solucion 1, pero con timestamp (luego lo eliminaremos)
        //avgspeed2 calcula las velocidades medias
        SingleOutputStreamOperator<Tuple4<Integer, Integer, Integer,Integer>> sumTumblingEventTimeWindows =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds((long) 3600))).apply(new AvgSpeed());


        KeyedStream<Tuple4<Integer, Integer, Integer,Integer>, Tuple> keyedStreamEx2=sumTumblingEventTimeWindows.keyBy(1);

        //a la solucioon 2 le pasamos la solucoion 1(tiene timestamp, podemos filtrar)
        //maxspeed2 coge la maxima velocidad
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> sol2 =
                keyedStreamEx2.window(TumblingEventTimeWindows.of(Time.seconds((long) 3600))).apply(new MaxSpeed());

        //eliminamos el ts de la sol1
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> sol1 =sumTumblingEventTimeWindows.map(new MapFunction<Tuple4<Integer, Integer, Integer,Integer>, Tuple3<Integer, Integer, Integer>>() {
                    public Tuple3<Integer, Integer, Integer> map(Tuple4<Integer, Integer, Integer,Integer> in) throws Exception{
                        Tuple3<Integer, Integer, Integer> out = new Tuple3(in.f0,
                               in.f1,in.f2);
                        return out;
                    }
                });




        // emit result
        if (params.has("output1")) {
            //sumTumblingEventTimeWindows1.writeAsText(params.get("output1"));
            sol1.writeAsCsv(params.get("output1"));
        }

        if (params.has("output2")) {
            sol2.writeAsCsv(params.get("output2"));
        }

        // execute program
        env.execute("exercise3");
    }



    public static class MaxSpeed implements WindowFunction<Tuple4<Integer, Integer, Integer,Integer>, Tuple3<Integer, Integer, Integer>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple4<Integer, Integer, Integer,Integer>> input, Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {
            Iterator<Tuple4<Integer, Integer, Integer,Integer>> iterator = input.iterator();
            Tuple4<Integer, Integer, Integer,Integer> first = iterator.next();

            Integer vid=0;
            Integer avgSpeed = 0;
            Integer xway=0;
            //Integer quantity=0;


            if (first != null) {
                vid = first.f0;

                avgSpeed=first.f2;
                xway=first.f1;
                //  quantity=1;
            }
            while (iterator.hasNext()) {
                Tuple4<Integer, Integer, Integer,Integer> next = iterator.next();
                if(next.f2>avgSpeed) {
                    avgSpeed = next.f2;
                    vid=next.f0;
                }
                //quantity += 1;
            }
            //avgSpeed=avgSpeed/quantity;
            out.collect(new Tuple3<Integer, Integer, Integer>(vid, xway, avgSpeed));
        }
    }

    public static class AvgSpeed implements WindowFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple4<Integer, Integer, Integer,Integer>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple5<Integer, Integer, Integer, Integer, Integer>> input, Collector<Tuple4<Integer, Integer, Integer,Integer>> out) throws Exception {
            Iterator<Tuple5<Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
            Tuple5<Integer, Integer, Integer, Integer, Integer> first = iterator.next();
            Integer ts = 0;
            Integer vid=0;
            Integer avgSpeed = 0;
            Integer xway=0;
            Integer quantity=0;


            if (first != null) {
                vid = first.f1;
                ts = first.f0;
                avgSpeed=first.f2;
                xway=first.f3;
                quantity=1;
            }
            while (iterator.hasNext()) {
                Tuple5<Integer, Integer, Integer, Integer, Integer> next = iterator.next();
                avgSpeed+=next.f2;
                quantity += 1;
            }
            avgSpeed=avgSpeed/quantity;
            out.collect(new Tuple4<Integer, Integer, Integer,Integer>(vid, xway, avgSpeed,ts));
        }
    }







}