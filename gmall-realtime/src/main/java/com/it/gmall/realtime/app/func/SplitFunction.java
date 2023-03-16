package com.it.gmall.realtime.app.func;

import com.it.gmall.realtime.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * @author ZuYingFang
 * @time 2023-03-14 9:56
 * @description 自定义flink sql函数，炸裂函数，也就是将一行变为多行，列数可以随意变化，看自己的需求
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

//    这是官网给的示例，可以参照着写
//    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
//    public static class SplitFunction extends TableFunction<Row> {
//
//        public void eval(String str) {
//            for (String s : str.split(" ")) {
//                // use collect(...) to emit a row
//                collect(Row.of(s, s.length()));
//            }
//        }
//    }

    public void eval(String str) {
        List<String> list = null;
        try {
            list = KeywordUtil.splitKeyword(str);
            for (String s : list) {
                collect(Row.of(s));
            }
        } catch (IOException e) {
            collect(Row.of(str));
        }

    }
}