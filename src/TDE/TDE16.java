package TDE;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class TDE16 {

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
        Job j = new Job(c, "tde16");

        //Registro de classes
        j.setJarByClass(TDE16.class);
        j.setMapperClass(Tde16Mapper.class);
        j.setReducerClass(Tde16Reducer.class);

        //Definição de tipos de saída
        //MAP
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(AverageMinMaxWritable.class);
        //REDUCE
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(AverageMinMaxWritable.class);

        // definicao de arquivos de entrada e saida
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class Tde16Mapper extends Mapper<Object, Text, Text, AverageMinMaxWritable> {
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {

            //Obtendo a linha para processamento
            String linha = value.toString();

            //Quebrando em campos
            String[] campos = linha.split(";");

            //Obtendo ano
            String ano = campos[1];
            String unidade = campos[7];

            String pais = campos[0];
            String categoria = campos[9];
            double preco = Double.parseDouble(campos[5]);

            //Emitir (chave, valor) → ("media", (n=1, sum=temperatura))
            if (pais.equals("Brazil")){
                context.write(new Text(ano+" "+unidade), new AverageMinMaxWritable(1, preco,preco,preco));
            }

        }
    }

    public static class Tde16Reducer extends Reducer<Text, AverageMinMaxWritable, Text, AverageMinMaxWritable> {

        public void reduce(Text key,
                           Iterable<AverageMinMaxWritable> values,
                           Context context) throws IOException, InterruptedException {

            int nTotal =0;
            double somaTotal = 0.0;
            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;

            for (AverageMinMaxWritable o: values){
                if(o.getMax()>max){
                    max = o.getMax();
                }
                if(o.getMin()<min){
                    min = o.getMin();
                }

                nTotal += o.getN();
                somaTotal += o.getSoma();
            }

            //Media = soma das somas/ soma dos Ns
            double media = somaTotal / nTotal;

            context.write(key, new AverageMinMaxWritable(1,media,min,max));

        }
    }

}
