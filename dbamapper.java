import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class dbaMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	private Text outKey = new Text();
    private DoubleWritable outVal = new DoubleWritable();

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip header
        if (line.startsWith("age")) return;

        String[] f = line.split(",");
        if (f.length < 31) return;

        try {
            double age = Double.parseDouble(f[0]);
            double bmi = Double.parseDouble(f[15]);
            double glucose = Double.parseDouble(f[24]);
            String outcome = f[30]; // 0 or 1

            outKey.set(outcome + "_Age");
            outVal.set(age);
            context.write(outKey, outVal);
            outKey.set(outcome + "_BMI");
            outVal.set(bmi);
            context.write(outKey, outVal);

            outKey.set(outcome + "_Glucose_Fasting");
            outVal.set(glucose);
            context.write(outKey, outVal);

        } catch (Exception e) {
            // skip malformed rows
        }
    }
}
