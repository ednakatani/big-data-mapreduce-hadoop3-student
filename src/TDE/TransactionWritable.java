package TDE;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TransactionWritable implements WritableComparable<TransactionWritable> {

    private Double max;
    private String commodity;

    public TransactionWritable(Double max, String commodity) {
        this.max = max;
        this.commodity = commodity;
    }

    public TransactionWritable() {

    }

    public Double getMax() {
        return max;
    }

    public void setMax(Double max) {
        this.max = max;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionWritable that = (TransactionWritable) o;
        return Objects.equals(max, that.max) && Objects.equals(commodity, that.commodity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(max, commodity);
    }

    @Override
    public String toString() {
        return "TransactionWritable{" +
                "max=" + max +
                ", commodity='" + commodity + '\'' +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(max);
        dataOutput.writeUTF(commodity);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        max = dataInput.readDouble();
        commodity = dataInput.readLine();


    }


    @Override
    public int compareTo(TransactionWritable o) {
        if (this.hashCode() > o.hashCode()){
            return +1;
        } else if (this.hashCode() < o.hashCode()) {
            return -1;
        }
        return 0;
    }
}
