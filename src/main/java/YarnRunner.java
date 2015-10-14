import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class YarnRunner extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new YarnRunner(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int numOfMaps = Integer.parseInt(args[0]);
        String cmd = args[1];
        String data = args[2];
        String cwd = args[3];

        cmd=cmd.replaceAll("TotalTask",args[0]);

        conf.setLong("numMaps", numOfMaps);
        conf.set("cmd", cmd);
        conf.set("cwd",cwd);

        String tmp = "/tmp";

        Path inPath = new Path(tmp, numOfMaps + "_run_" + numOfMaps);
        Path outPath = new Path(tmp, numOfMaps + "_run_out_" + numOfMaps);
        writeNLineHDFSFile(conf, inPath, numOfMaps);

        Job job = Job.getInstance(conf, "Run " + numOfMaps + " [ " + cmd + " ] ");
        job.setJarByClass(YarnRunner.class);
        job.setMapperClass(ExecMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(NLineInputFormat.class);
        job.setNumReduceTasks(0);
        job.addCacheFile(new URI(data));

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        job.submit();
        job.waitForCompletion(true);
        if (conf.getBoolean("deleteonExit", true)) {
            FileSystem hdfs = FileSystem.get(conf);
            hdfs.delete(inPath, true);
            hdfs.delete(outPath, true);
        }

        return 1;
    }

    public static class ExecMapper extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String cmd = context.getConfiguration().get("cmd");
            cmd=cmd.replaceAll("TaskNum", value.toString());
            System.out.println("runnning : " + cmd);
            String cwd = context.getConfiguration().get("cwd");

            Process process = null;
            try {
                ProcessBuilder processBuilder = new ProcessBuilder(cmd.split(" "));
                if (cwd != null) {
                    processBuilder.directory(new File(cwd));
                }
                processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
                processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
                process = processBuilder.start();
                process.waitFor();
            } catch (Exception e) {
                System.err.println(e);
            } finally {
                if (process != null) {
                    process.destroy();
                }
            }
        }
    }
    public static void writeNLineHDFSFile(Configuration conf, Path path, int N) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FSDataOutputStream writer = hdfs.create(path);
        for (int i = 1; i <= N; i++) {
            writer.write(String.format("%d\n",i).getBytes());
        }
        writer.close();
    }

}

