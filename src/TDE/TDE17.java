package TDE;

import advanced.customwritable.AverageTemperature;
import advanced.customwritable.FireAvgTempWritable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TDE17 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        Path intermediate = new Path("./output/intermediate.tmp");

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "busca");

        //Registro de classes
        j.setJarByClass(TDE17.class);
        j.setMapperClass((MapA.class));
        j.setReducerClass(ReduceA.class);

        //Definição de tipos de saida
        //Map
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(DoubleWritable.class);
        //Reduce
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        // definicao de arquivos de entrada e saida
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j, intermediate);

        // lanca o job e aguarda sua execucao
        if(!j.waitForCompletion(true)){
            System.err.println("ERRO JOB 1");
            return;
        }


        // criacao do job e seu nome
        Job j2 = new Job(c, "maximo");

        //Registro de classes
        j2.setJarByClass(TDE17.class);
        j2.setMapperClass((MapB.class));
        j2.setReducerClass(ReduceB.class);

        //Definição de tipos de saida
        //Map
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(TransactionWritable.class);
        //Reduce
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);

        // definicao de arquivos de entrada e saida
        FileInputFormat.addInputPath(j2,intermediate);
        FileOutputFormat.setOutputPath(j2, output);

        // lanca o job e aguarda sua execucao
        if(!j2.waitForCompletion(true)){
            System.err.println("ERRO JOB 2");
            return;
        }
    }


    public static class MapA extends Mapper<Object, Text, Text, DoubleWritable> {
        public void map(Object key, Text value, Context con)
                throws IOException, InterruptedException {

            //Obtendo a linha para processamento
            String linha = value.toString();

            //Quebrando em campos
            String[] campos = linha.split(";");

            //Obtendo ano
            String year = campos[1];
            String flow = campos[4];
            String commodity = campos[3];
            double ammount = Double.parseDouble(campos[8]);

            //Emitir (chave, valor) → ("media", (n=1, sum=temperatura))
            if(Objects.equals(year, "2016")){
                con.write(new Text(flow+"\t"+commodity), new DoubleWritable(ammount));
            }

        }
    }

    public static class ReduceA extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {

            long sum = 0;
            for (DoubleWritable v : values){
                sum += v.get();
            }

            //Salvando em arquivo
            con.write(key, new DoubleWritable(sum));

        }
    }


    public static class MapB extends Mapper<Object, Text, Text, TransactionWritable> {
        public void map(Object key, Text value, Context con)
                throws IOException, InterruptedException {

            //Obtendo a linha para processamento
            String linha = value.toString();

            //Quebrando em campos
            String[] campos = linha.split("\t");

            //Obtendo fluxo, commodity e ammount
            String flow = campos[0];
            String commodity = campos[1];
            double ammount = Double.parseDouble(campos[2]);

            //Selecionando as transações por fluxo
            con.write(new Text(flow), new TransactionWritable(ammount,commodity));


        }
    }

    public static class ReduceB extends Reducer<Text, TransactionWritable, Text, Text> {
        public void reduce(Text key, Iterable<TransactionWritable> values, Context con)
                throws IOException, InterruptedException {


            String nome_max = "";
            double max = Double.MIN_VALUE;

            //Verifica o maior valor por fluxo
            for (TransactionWritable o: values){
                if(o.getMax()>max){
                    max = o.getMax();
                    nome_max = o.getCommodity();
                }

            }

            con.write(key, new Text(nome_max+"\t"+max));

        }
    }

}
