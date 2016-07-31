package com.example;

/**
 * Created by shaobo on 7/30/16.
 */
public interface StudentDao {
    void insert(StudentDo stu);
    void delete(String id);
    void update(StudentDo stu);
    StudentDo query(String id);
}
