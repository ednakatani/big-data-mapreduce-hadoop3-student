package advanced.entropy;

import advanced.customwritable.FireAvgTempWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class BaseQtdWritable implements WritableComparable<BaseQtdWritable>{

    private int base_a;
    private int base_c;
    private int base_t;
    private int base_g;

    private int total;

    public BaseQtdWritable() {
    }

    public BaseQtdWritable(int base_a, int base_c, int base_t, int base_g) {
        this.base_a = base_a;
        this.base_c = base_c;
        this.base_t = base_t;
        this.base_g = base_g;
        this.total = base_a + base_c + base_g + base_t;
    }

    public int getBase_a() {
        return base_a;
    }

    public void setBase_a(int base_a) {
        this.base_a = base_a;
    }

    public int getBase_c() {
        return base_c;
    }

    public void setBase_c(int base_c) {
        this.base_c = base_c;
    }

    public int getBase_t() {
        return base_t;
    }

    public void setBase_t(int base_t) {
        this.base_t = base_t;
    }

    public int getBase_g() {
        return base_g;
    }

    public void setBase_g(int base_g) {
        this.base_g = base_g;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BaseQtdWritable that = (BaseQtdWritable) o;
        return base_a == that.base_a && base_c == that.base_c && base_t == that.base_t && base_g == that.base_g && total == that.total;
    }

    @Override
    public int hashCode() {
        return Objects.hash(base_a, base_c, base_t, base_g, total);
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public String toString() {
        return "A "+this.base_a+"\nC "+this.base_c+"\nG "+this.base_g+"\nT "+this.base_t;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
    }

    @Override
    public int compareTo(BaseQtdWritable o) {
        if (this.hashCode() > o.hashCode()){
            return +1;
        } else if (this.hashCode() < o.hashCode()) {
            return -1;
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(base_a);
        dataOutput.writeInt(base_c);
        dataOutput.writeInt(base_t);
        dataOutput.writeInt(base_g);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        base_a = dataInput.readInt();
        base_c = dataInput.readInt();
        base_t = dataInput.readInt();
        base_g = dataInput.readInt();


    }
}