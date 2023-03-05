import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;

public class Dijkstra {

    public static class DijkstraMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // A	0_B:4,C:2,D:5
            String[] split = value.toString().split("_");// A   0 and B:4,C:2,D:5

            String[] node = split[0].split("\t"); //A 0
            String nId = node[0];// A
            int cost = Integer.parseInt(node[1]); // 0

            String[] costAndNodes = split[1].split(","); // B:4 C:2 D:5

            if (cost != -1) {
                for (String s : costAndNodes) {
                    if (!s.equals(" ")) {
                        String[] costAndNode = s.split(":"); // B 4
                        String v_node = costAndNode[0];// B
                        int v_cost = Integer.parseInt(costAndNode[1]); // 4

                        context.write(new Text(v_node), new Text(v_cost + cost + "_" + " "));
                        // <B, newCost_ >
                    }
                }
            }

            context.write(new Text(nId), new Text(cost + "_" + split[1]));// <A, (0_B:4,C:2,D:5)>
        }
    }

    public static class DijkstraReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int lowCost = -1;
            String s_costAndNodes = " ";

            for (Text value : values) {
                // newCost_ or newCost_B:4,C:2,D:5
                String[] parts = value.toString().split("_");// newCost 空格 | newCost and B:4,C:2,D:5
                int cost = Integer.parseInt(parts[0]); // newCost
                String costAndNodes = parts[1];// 空格 or B:4,C:2,D:5

                if (cost != -1) // 如果cost不是-1
                {
                    if (lowCost == -1) {
                        lowCost = cost;
                    }
                    else {
                        if (cost < lowCost) {
                            lowCost = cost;
                        }
                    }
                }

                if (!costAndNodes.equals(" ")) {
                    s_costAndNodes = costAndNodes;
                }
            }
            context.write(key, new Text(lowCost + "_" + s_costAndNodes));// <B, (newCost_C:2,D:3)>
            // <A,...> A的永远不会变
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String out = "/part-r-00000";

        int i = 0;
        while (true) {
            i++;
            Job job = Job.getInstance(conf, "Dijkstra");
            job.setJarByClass(Dijkstra.class);
            job.setJobName("MapReduce" + i);

            job.setMapperClass(DijkstraMapper.class);
            job.setReducerClass(DijkstraReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            if(i == 1) { FileInputFormat.addInputPath(job, new Path(args[0])); }
            else { FileInputFormat.addInputPath(job, new Path(args[1] + (i - 1) + out)); }

            FileOutputFormat.setOutputPath(job, new Path(args[1] + i));

            if(!job.waitForCompletion(true)) { System.exit(1); }

            if(i != 1){
                File anterior = new File(args[1] + (i - 1) + out);
                File actual = new File(args[1] + i + out);
                if(FileUtils.contentEquals(anterior, actual)) { break; }
            }

        }

        System.exit(0);
    }

}
