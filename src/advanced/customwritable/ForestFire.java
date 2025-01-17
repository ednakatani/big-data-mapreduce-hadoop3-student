package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;

public class ForestFire {

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
        Job j = new Job(c, "forestfire");

        //Registro de classes
        j.setJarByClass(ForestFire.class);
        j.setMapperClass(ForestFireMapper.class);
        j.setReducerClass(ForestFireReducer.class);

        //Definição de tipos de saída
        //MAP
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(ForestFireWritable.class);
        //REDUCE
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(ForestFireWritable.class);

        // definicao de arquivos de entrada e saida
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class ForestFireMapper extends Mapper<Object, Text, Text, ForestFireWritable> {
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {

            //Obtendo a linha para processamento
            String linha = value.toString();

            //Quebrando em campos
            String[] campos = linha.split(",");

            //Obtendo temperatura
            double temp = Double.parseDouble(campos[8]);
            double wind = Double.parseDouble(campos[10]);
            String month = campos[2];

            //Emitir (chave, valor) → ("media", (n=1, sum=temperatura))
            context.write(new Text(month), new ForestFireWritable(temp,wind));
            //con.write(new Text("mediawind"), new FireAvgTempWritable(1,vento));
        }
    }

    public static class ForestFireReducer extends Reducer<Text, ForestFireWritable, Text, ForestFireWritable> {

        public void reduce(Text key,
                           Iterable<ForestFireWritable> values,
                           Context context) throws IOException, InterruptedException {

            // Verifica o maior valor de cada dado
            double max_wind = Double.MIN_VALUE;
            double max_temp = Double.MIN_VALUE;


            for (ForestFireWritable o: values) {

                if (o.getTemp() > max_temp) {
                    max_temp = o.getTemp();
                }
                if (o.getWind() > max_wind) {
                    max_wind = o.getWind();
                }
            }

            //Text ktemp = new Text("temp-"+key);
            //Text kwind = new Text("wind-"+key);

            //context.write(ktemp, new DoubleWritable(bigger_temp));
            //context.write(kwind, new DoubleWritable(max_wind));

            ForestFireWritable r = new ForestFireWritable(max_temp,max_wind);

            context.write(key, r);

        }
    }

}
