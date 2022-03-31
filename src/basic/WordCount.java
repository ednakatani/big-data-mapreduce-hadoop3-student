package basic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


public class WordCount {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/bible.txt");

        // arquivo de saida
        Path output = new Path("output/contagem.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "wordcount");

        // registro de classes (main, map, reduce)
        j.setJarByClass(WordCount.class);
        j.setMapperClass(MapForWordCount.class);
        j.setReducerClass(ReduceForWordCount.class);

        //tipos de saida (map e reduce)
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        //cadastro de arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // executa a rotina
        j.waitForCompletion(false);
    }

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            //convertendo para string
            String linha = value.toString();

            // quebrando a linha em palavras
            String[] palavras = linha.split(" " );

            // para cada palavra, gerar (palavra, 1)
            for(String p : palavras){
                con.write(new Text(p), new IntWritable(1));
            }
        }
    }

    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {


        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            //a função de reduce sera chamada por chave

            //somar os valores que estao na lista
            int sum = 0;
            for (IntWritable v : values){
                sum += v.get(); //get retorna o valor em int tradicional
            }

            //Salvando em arquivo
            con.write(key, new IntWritable(sum));

        }
    }

}
