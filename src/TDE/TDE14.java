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

public class TDE14 {

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
        Job j = new Job(c, "tde14");

        //Registro de classes
        j.setJarByClass(TDE14.class);
        j.setMapperClass(Tde14Mapper.class);
        j.setReducerClass(Tde14Reducer.class);

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

    public static class Tde14Mapper extends Mapper<Object, Text, Text, AverageWritable> {
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {

            //Obtendo a linha para processamento
            String linha = value.toString();

            //Quebrando em campos
            String[] campos = linha.split(";");

            //Obtendo ano e preço
            String ano = campos[1];
            double preco = Double.parseDouble(campos[5]);

            //Enviando os preços de cada ano
            context.write(new Text(ano), new AverageWritable(1,preco));
        }
    }

    public static class Tde14Reducer extends Reducer<Text, AverageWritable, Text, DoubleWritable> {

        public void reduce(Text key,
                           Iterable<AverageWritable> values,
                           Context context) throws IOException, InterruptedException {

            int nTotal =0;
            double somaTotal = 0.0;

            //Calculando a média
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
