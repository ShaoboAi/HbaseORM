package com.example;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.Map;

/**
 * Hello world!
 *
 */
public class App 
{

    public static void main( String[] args )
    {

        StudentDao studentDao = new StudentDaoImpl();
        StudentDo stu = new StudentDo();
        stu.setName("aiqin");
        stu.setAge(19);
        stu.setLessons(Lists.newArrayList("math", "chemistry", "physics"));
        Map<String, Integer> s = new HashMap<>();
        s.put("english", 100);
        stu.setScores(s);
        studentDao.insert(stu);

        s = new HashMap<>();
        s.put("sports", 100);
        stu.setScores(s);
        studentDao.update(stu);
        studentDao.delete(stu.rowKey());
        System.out.println(JSON.toJSONString(studentDao.query(stu.rowKey())));

        System.out.println( "Hello World!" );
    }
}
