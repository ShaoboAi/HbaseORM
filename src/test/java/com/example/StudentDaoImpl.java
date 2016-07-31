package com.example;


import com.pingnotes.support.HbaseDaoSupport;

/**
 * Created by shaobo on 7/30/16.
 */
public class StudentDaoImpl extends HbaseDaoSupport<StudentDo> implements StudentDao {
    private static final String tableName = "test";
    public StudentDaoImpl(){
        super(tableName, StudentDo.class);
    }

    @Override
    public void insert(StudentDo stu) {
        super.insert(stu);
    }

    @Override
    public void delete(String id) {
        super.delete(id);
    }

    @Override
    public void update(StudentDo stu) {
        super.update(stu);
    }

    @Override
    public StudentDo query(String id) {
        return super.query(id);
    }
}
