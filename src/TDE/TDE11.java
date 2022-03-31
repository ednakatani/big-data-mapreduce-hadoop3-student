package TDE;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;

public class TDE11 {

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
        Job j = new Job(c, "tde11");

        //Registro de classes
        j.setJarByClass(TDE11.class);
        j.setMapperClass(Tde11Mapper.class);
        j.setReducerClass(Tde11Reducer.class);

        //Definição de tipos de saída
        //MAP
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        //REDUCE
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // definicao de arquivos de entrada e saida
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class Tde11Mapper extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {

            //Obtendo a linha para processamento
            String linha = value.toString();

            //Quebrando em campos
            String[] campos = linha.split(";");

            //Obtendo pais
            String pais = campos[0];

            //Emitir (chave, valor) → ("media", (n=1, sum=temperatura))
            if (pais.equals("Brazil")){
                context.write(new Text(pais), new IntWritable(1));
            }
            //con.write(new Text("mediawind"), new FireAvgTempWritable(1,vento));
        }
    }

    public static class Tde11Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key,
                           Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values){
                sum += v.get(); //get retorna o valor em int tradicional
            }

            context.write(key, new IntWritable(sum));

        }
    }

}
