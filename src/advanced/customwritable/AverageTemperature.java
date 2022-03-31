package advanced.customwritable;

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

import java.io.IOException;

public class AverageTemperature {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "media");

        //Registro de classes
        j.setJarByClass(AverageTemperature.class);
        j.setMapperClass((MapForAverage.class));
        j.setReducerClass(ReduceForAverage.class);

        //Definição de tipos de saida
        //Map
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FireAvgTempWritable.class);
        //Reduce
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        // definicao de arquivos de entrada e saida
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //Obtendo a linha para processamento
            String linha = value.toString();

            //Quebrando em campos
            String campos[] = linha.split(",");

            //Obtendo temperatura
            double temperatura = Double.parseDouble(campos[8]);
            //double vento = Double.parseDouble(campos[10]);
            String mes = campos[2];

            //Emitir (chave, valor) → ("media", (n=1, sum=temperatura))
            con.write(new Text("mediatemp"+mes), new FireAvgTempWritable(1,temperatura));
            //con.write(new Text("mediawind"), new FireAvgTempWritable(1,vento));


        }
    }

    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {

            //Receber (chave, lista de valores)
            //(chave="media", [...........])
            // cada valor é um objeto

            // laco de repeticao
            // somar os Ns e somar as somas

            int nTotal =0;
            double somaTotal = 0.0;
            for (FireAvgTempWritable o: values){
                nTotal += o.getN();
                somaTotal += o.getSoma();
            }

            //Media = soma das somas/ soma dos Ns
            double media = somaTotal / nTotal;

            con.write(key, new DoubleWritable(media));
        }
    }

}
