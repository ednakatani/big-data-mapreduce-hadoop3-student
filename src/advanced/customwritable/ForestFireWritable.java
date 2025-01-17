package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ForestFireWritable implements WritableComparable<ForestFireWritable> {

    private double temp;
    private double wind;

    public ForestFireWritable() {
    }

    public ForestFireWritable(double temp, double wind) {
        this.temp = temp;
        this.wind =  wind;
    }

    public String print(){
        return getTemp()+" "+getWind();
    }

    public double getTemp() {
        return this.temp;
    }

    public void setTemp(double temp) {
        this.temp = temp;
    }

    public double getWind() {
        return this.wind;
    }

    public void setWind(double wind) {
        this.wind = wind;
    }


    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ForestFireWritable that = (ForestFireWritable) o;
        return Double.compare(that.temp, temp) == 0 && Double.compare(that.wind, wind) == 0;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public String toString() {
        return "maxtemp: " +getTemp()+" maxwind: "+getWind();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
    }

    @Override
    public int compareTo(ForestFireWritable o) {
        if (this.hashCode() > o.hashCode()){
            return +1;
        } else if (this.hashCode() < o.hashCode()) {
            return -1;
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(temp);
        dataOutput.writeDouble(wind);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        temp = dataInput.readDouble();
        wind = dataInput.readDouble();
    }

}
