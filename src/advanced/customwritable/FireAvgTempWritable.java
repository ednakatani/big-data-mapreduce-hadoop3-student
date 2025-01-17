package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

public class FireAvgTempWritable implements WritableComparable<FireAvgTempWritable> {
    /**
     * Todo writable precisa ser um Java BEAN
     * 1- Construtor Vazio
     * 2- Gets e Sets
     * 3- Comparação entre objetos
     * 4- Atributos Privados
     */

    private int n;
    private double soma;


    public FireAvgTempWritable() {
    }

    public FireAvgTempWritable(int n, double soma) {
        this.n = n;
        this.soma = soma;
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

    @Override
    public int compareTo(FireAvgTempWritable o) {
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
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        n = dataInput.readInt();
        soma = dataInput.readDouble();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FireAvgTempWritable that = (FireAvgTempWritable) o;
        return n == that.n && Double.compare(that.soma, soma) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(n, soma);
    }
}
