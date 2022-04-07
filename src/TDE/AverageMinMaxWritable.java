package TDE;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class AverageMinMaxWritable implements WritableComparable<AverageMinMaxWritable> {
    /**
     * Todo writable precisa ser um Java BEAN
     * 1- Construtor Vazio
     * 2- Gets e Sets
     * 3- Comparação entre objetos
     * 4- Atributos Privados
     */

    private int n;
    private double soma;
    private double min;
    private double max;


    public AverageMinMaxWritable() {
    }

    public AverageMinMaxWritable(int n, double soma, double min, double max) {
        this.n = n;
        this.soma = soma;
        this.max = max;
        this.min = min;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public double getSoma() {
        return soma;
    }

    public void setSoma(double soma) {
        this.soma = soma;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public void setMax(double max) {
        this.max = max;
    }


    @Override
    public int compareTo(AverageMinMaxWritable o) {
        if (this.hashCode() > o.hashCode()){
            return +1;
        } else if (this.hashCode() < o.hashCode()) {
            return -1;
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(n);
        dataOutput.writeDouble(soma);
        dataOutput.writeDouble(min);
        dataOutput.writeDouble(max);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        n = dataInput.readInt();
        soma = dataInput.readDouble();
        min = dataInput.readDouble();
        max = dataInput.readDouble();

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AverageMinMaxWritable that = (AverageMinMaxWritable) o;
        return n == that.n && that.soma == soma && that.min == min && that.max == max;
    }

    @Override
    public int hashCode() {
        return Objects.hash(n, soma);
    }

    @Override
    public String toString() {
        return "max: " +getMax()+" min: "+getMin()+"  soma: "+getSoma();
    }

}
