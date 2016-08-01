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
        stu.setName("baobao");
        stu.setAge(20);
        stu.setLessons(Lists.newArrayList("math", "chemistry", "physics"));
        Map<String, Integer> s = new HashMap<>();
        s.put("english", 100);
        stu.setScores(s);
        studentDao.insert(stu);
        System.out.println("after insert, " + JSON.toJSONString(studentDao.query(stu.rowKey())));

//        s = new HashMap<>();
//        s.put("sports", 100);
//        stu.setScores(s);
//        stu.setLessons(Lists.newArrayList());
//        studentDao.update(stu);
//        System.out.println("after update, " + JSON.toJSONString(studentDao.query(stu.rowKey())));


        studentDao.delete(stu.rowKey());

        System.out.println(JSON.toJSONString(studentDao.query(stu.rowKey())));

        System.out.println( "Hello World!" );
    }
}
