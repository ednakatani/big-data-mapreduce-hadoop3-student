package TDE;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class TDE15 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);
        //Path inputB = new Path(files[1]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "tde15");

        //Registro de classes
        j.setJarByClass(TDE15.class);
        j.setMapperClass(Tde15Mapper.class);
        j.setReducerClass(Tde15Reducer.class);

        //Definição de tipos de saída
        //MAP
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(AverageWritable.class);
        //REDUCE
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        // definicao de arquivos de entrada e saida
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class Tde15Mapper extends Mapper<Object, Text, Text, AverageWritable> {
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {

            //Obtendo a linha para processamento
            String linha = value.toString();

            //Quebrando em campos
            String[] campos = linha.split(";");

            //Obtendo ano
            String ano = campos[1];
            String pais = campos[0];
            String unidade = campos[7];
            String categoria = campos[9];
            double preco = Double.parseDouble(campos[5]);

            //Emitir (chave, valor) → ("media", (n=1, sum=temperatura))
            if (pais.equals("Brazil")){
                context.write(new Text(ano+"\t"+categoria+"\t"+unidade), new AverageWritable(1, preco));
            }

        }
    }

    public static class Tde15Reducer extends Reducer<Text, AverageWritable, Text, DoubleWritable> {

        public void reduce(Text key,
                           Iterable<AverageWritable> values,
                           Context context) throws IOException, InterruptedException {

            int nTotal =0;
            double somaTotal = 0.0;
            for (AverageWritable o: values){
                nTotal += o.getN();
                somaTotal += o.getSoma();
            }

            //Media = soma das somas/ soma dos Ns
            double media = somaTotal / nTotal;

            context.write(key, new DoubleWritable(media));

        }
    }

}
