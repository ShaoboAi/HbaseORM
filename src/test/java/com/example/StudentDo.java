package com.example;


import com.pingnotes.annotation.Column;
import com.pingnotes.annotation.JsonField;
import com.pingnotes.annotation.MapField;
import com.pingnotes.support.HbaseBaseDo;

import java.util.List;
import java.util.Map;


/**
 * Created by shaobo on 7/30/16.
 */

public class StudentDo extends HbaseBaseDo {
    @Column(family = "i", qualifier = "name")
    private String name;

    @Column(family = "i", qualifier = "age")
    private Integer age;

    @Column(family = "i", qualifier = "lessons")
    @JsonField(elementClass = String.class)
    private List<String> lessons;

    @Column(family = "s", qualifier = "scores")
    @MapField(keyClass = String.class, valueClass = Integer.class)
    private Map<String, Integer> scores;

    @Override
    public String rowKey() {
        return name + ":" + age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public List<String> getLessons() {
        return lessons;
    }

    public void setLessons(List<String> lessons) {
        this.lessons = lessons;
    }

    public Map<String, Integer> getScores() {
        return scores;
    }

    public void setScores(Map<String, Integer> scores) {
        this.scores = scores;
    }
}
