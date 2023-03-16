package com.it.gmall.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ZuYingFang
 * @time 2023-03-14 9:43
 * @description 切分我们的词，也就是将一个字符串根据特定的规则切分成几个字串，这个规则是自带的
 */
public class KeywordUtil {

    public static List<String> splitKeyword(String keyWord) throws IOException {

        //创建集合用于存放切分后的数据
        ArrayList<String> list = new ArrayList<>();

        //创建IK分词对象
        // ik_smart  第二个参数填写true时，规则就是ik_smart，输出为：[大数, 据, 项目, 之, flink, 实, 时数, 仓]
        // ik_max_word  第二个参数填写false时，规则就是ik_max_word，输出为：[大数, 数据项, 数据, 项目, 之, flink, 实时, 时数, 仓]
        // 可见，false更适合我们普通的语义
        StringReader reader = new StringReader(keyWord);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        //循环取出切分好的词
        Lexeme next = ikSegmenter.next();

        while (next != null) {
            String text = next.getLexemeText();
            list.add(text);
            next = ikSegmenter.next();
        }


        //最终返回集合
        return list;

    }

    public static void main(String[] args) throws IOException {
        System.out.println(splitKeyword("大数据项目之Flink实时数仓"));
    }


}
