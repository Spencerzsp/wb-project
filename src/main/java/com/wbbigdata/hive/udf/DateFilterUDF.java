package com.wbbigdata.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class DateFilterUDF extends UDF {

    public Text evaluate(Text input){
        return new Text("Hello " + input);
    }

    public static void main(String[] args) {
        DateFilterUDF udf = new DateFilterUDF();
        Text text = udf.evaluate(new Text("zhangsan"));
        System.out.println(text);
    }
}
