package advanced.entropy;

import advanced.customwritable.AverageTemperature;
import advanced.customwritable.FireAvgTempWritable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EntropyFASTA {

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
        Job j = new Job(c, "contagem");

        //Registro de classes
        j.setJarByClass(EntropyFASTA.class);
        j.setMapperClass((MapEtapaA.class));
        j.setReducerClass(ReduceEtapaA.class);

        //Definição de tipos de saida
        //Map
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(BaseQtdWritable.class);
        //Reduce
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(BaseQtdWritable.class);

        // definicao de arquivos de entrada e saida
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);

    }


    public static class MapEtapaA extends Mapper<LongWritable, Text, Text, BaseQtdWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //Obtendo a linha para processamento
            String linha = value.toString();

            //Ignora primeira linha
            if(linha.startsWith(">"))return;

            //Conta A C T G / linha
            int cont_a = StringUtils.countMatches(linha, "A");
            int cont_c = StringUtils.countMatches(linha, "C");
            int cont_t = StringUtils.countMatches(linha, "T");
            int cont_g = StringUtils.countMatches(linha, "G");

            con.write(new Text("chave"), new BaseQtdWritable(cont_a,cont_c,cont_t,cont_g));
        }
    }

    public static class ReduceEtapaA extends Reducer<Text, BaseQtdWritable, Text, BaseQtdWritable> {
        public void reduce(Text key, Iterable<BaseQtdWritable> values, Context con)
                throws IOException, InterruptedException {

            int cont_a = 0;
            int cont_c = 0;
            int cont_t = 0;
            int cont_g = 0;

            for (BaseQtdWritable v: values) {
                cont_a += v.getBase_a();
                System.out.println("\n\n\n\n\n\n"+v.getBase_a());
                cont_c += v.getBase_c();
                cont_g += v.getBase_g();
                cont_t += v.getBase_t();
            }

            con.write(key, new BaseQtdWritable(cont_a,cont_c,cont_t,cont_g));

        }
    }


    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, BaseQtdWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

        }
    }

    public static class ReduceEtapaB extends Reducer<Text, BaseQtdWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<BaseQtdWritable> values, Context con)
                throws IOException, InterruptedException {

        }
    }

}
