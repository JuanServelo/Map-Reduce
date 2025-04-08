package TDE2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.chainsaw.Main;

import java.io.IOException;

public class MapReduce {
    public static class MapQuestao1 extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String linha = value.toString();
            if (linha.startsWith("country_or_area")) return;
            String[] campos = linha.split(";");
            if (campos.length > 0 && campos[0].equalsIgnoreCase("Brazil")) {
                context.write(new Text("Numero de transações do Brasil"), new LongWritable(1));
            }
        }
    }

    public static class MapQuestao2 extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String linha = value.toString();
            if (linha.startsWith("country_or_area")) return;
            String[] campos = linha.split(";");
            if (campos.length > 1) {
                context.write(new Text(campos[1]), new LongWritable(1));
            }
        }
    }

    public static class MapQuestao3 extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String linha = value.toString();
            if (linha.startsWith("country_or_area")) return;
            String[] campos = linha.split(";");
            if (campos.length > 9) {
                context.write(new Text(campos[9]), new LongWritable(1));
            }
        }
    }

    public static class MapQuestao4 extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String linha = value.toString();
            if (linha.startsWith("country_or_area")) return;
            String[] campos = linha.split(";");
            if (campos.length > 4) {
                context.write(new Text(campos[4]), new LongWritable(1));
            }
        }
    }

    public static class MapQuestao5 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String linha = value.toString();
            if (linha.startsWith("country_or_area")) return;
            String[] campos = linha.split(";");
            if (campos.length > 5 && campos[0].equalsIgnoreCase("Brazil")) {
                String ano = campos[1];
                try {
                    double valor = Double.parseDouble(campos[5]);
                    context.write(new Text(ano), new DoubleWritable(valor));
                } catch (NumberFormatException ignored) {}
            }
        }
    }

    public static class MapQuestao6 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String linha = value.toString();
            if (linha.startsWith("country_or_area")) return;
            String[] campos = linha.split(";");
            if (campos.length > 5 && campos[0].equalsIgnoreCase("Brazil") && campos[1].equals("2016")) {
                try {
                    double valor = Double.parseDouble(campos[5]);
                    context.write(new Text("Brasil_2016"), new DoubleWritable(valor));
                } catch (NumberFormatException ignored) {}
            }
        }
    }

    public static class MapQuestao7 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String linha = value.toString();
            if (linha.startsWith("country_or_area")) return;
            String[] campos = linha.split(";");
            if (campos.length > 5 && campos[0].equalsIgnoreCase("Brazil") && campos[4].equalsIgnoreCase("Export")) {
                try {
                    double valor = Double.parseDouble(campos[5]);
                    context.write(new Text(campos[1]), new DoubleWritable(valor));
                } catch (NumberFormatException ignored) {}
            }
        }
    }

    public static class MapQuestao8 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String linha = value.toString();
            if (linha.startsWith("country_or_area")) return;
            String[] campos = linha.split(";");
            if (campos.length > 5) {
                try {
                    double valor = Double.parseDouble(campos[5]);
                    String chave = campos[1] + "_" + campos[0];
                    context.write(new Text(chave), new DoubleWritable(valor));
                } catch (NumberFormatException ignored) {}
            }
        }
    }

    public static class ReduceContagem extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long total = 0;
            for (LongWritable val : values) {
                total += val.get();
            }
            context.write(key, new LongWritable(total));
        }
    }

    public static class ReduceMedia extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double soma = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                soma += val.get();
                count++;
            }
            context.write(key, new DoubleWritable(soma / count));
        }
    }

    public static class ReduceMaxMin extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;
            for (DoubleWritable val : values) {
                double v = val.get();
                if (v > max) max = v;
                if (v < min) min = v;
            }
            context.write(new Text(key.toString() + "Transação mais cara:"), new DoubleWritable(max));
            context.write(new Text(key.toString() + "Transação mais barata:"), new DoubleWritable(min));
        }
    }

    public static class ReduceMaxMinAnoPais extends Reducer<Text, DoubleWritable, Text, Text> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;
            for (DoubleWritable val : values) {
                double v = val.get();
                if (v > max) max = v;
                if (v < min) min = v;
            }
            context.write(key, new Text("Menor valor: " + min + ", Maior valor: " + max));
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        for(int i = 1; i < 9; i++){

            String questao = (""+i);
            Path input = new Path("./in/operacoes_comerciais_inteira.csv");
            Path output = new Path("./output/respostaquestão"+i+".txt");

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Analise Questao " + questao);
            job.setJarByClass(Main.class);

            switch (questao) {
                case "1":
                    job.setMapperClass(MapQuestao1.class);
                    job.setReducerClass(ReduceContagem.class);
                    job.setMapOutputKeyClass(Text.class);
                    job.setMapOutputValueClass(LongWritable.class);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(LongWritable.class);
                    break;
                case "2":
                    job.setMapperClass(MapQuestao2.class);
                    job.setReducerClass(ReduceContagem.class);
                    job.setMapOutputKeyClass(Text.class);
                    job.setMapOutputValueClass(LongWritable.class);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(LongWritable.class);
                    break;
                case "3":
                    job.setMapperClass(MapQuestao3.class);
                    job.setReducerClass(ReduceContagem.class);
                    job.setMapOutputKeyClass(Text.class);
                    job.setMapOutputValueClass(LongWritable.class);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(LongWritable.class);
                    break;
                case "4":
                    job.setMapperClass(MapQuestao4.class);
                    job.setReducerClass(ReduceContagem.class);
                    job.setMapOutputKeyClass(Text.class);
                    job.setMapOutputValueClass(LongWritable.class);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(LongWritable.class);
                    break;
                case "5":
                    job.setMapperClass(MapQuestao5.class);
                    job.setReducerClass(ReduceMedia.class);
                    job.setMapOutputKeyClass(Text.class);
                    job.setMapOutputValueClass(DoubleWritable.class);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(DoubleWritable.class);
                    break;
                case "6":
                    job.setMapperClass(MapQuestao6.class);
                    job.setReducerClass(ReduceMaxMin.class);
                    job.setMapOutputKeyClass(Text.class);
                    job.setMapOutputValueClass(DoubleWritable.class);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(DoubleWritable.class);
                    break;
                case "7":
                    job.setMapperClass(MapQuestao7.class);
                    job.setReducerClass(ReduceMedia.class);
                    job.setMapOutputKeyClass(Text.class);
                    job.setMapOutputValueClass(DoubleWritable.class);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(DoubleWritable.class);
                    break;
                case "8":
                    job.setMapperClass(MapQuestao8.class);
                    job.setReducerClass(ReduceMaxMinAnoPais.class);
                    job.setMapOutputKeyClass(Text.class);
                    job.setMapOutputValueClass(DoubleWritable.class);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(Text.class);
                    break;
                default:
                    System.out.println("Finalizado.");
                    System.exit(1);
            }

            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);

            job.setNumReduceTasks(1);
            job.waitForCompletion(true);
        }
    }
}
