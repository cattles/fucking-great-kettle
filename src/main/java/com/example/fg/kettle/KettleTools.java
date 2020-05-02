package com.example.fg.kettle;


import lombok.extern.slf4j.Slf4j;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * @author cattle -  稻草鸟人
 * @date 2020/4/30 下午5:09
 */
@Slf4j
public class KettleTools {


    public static void runJob(String jobName, Map<String, String> variable) {
        try {
            KettleEnvironment.init();
            JobMeta jobMeta = new JobMeta(jobName, null);
            Job job = new Job(null, jobMeta);
            variable.forEach((key, value) -> {
                job.setVariable(key, value);
            });
            job.start();
            job.waitUntilFinished();
            if (job.getErrors() > 0) {
                throw new Exception("There were errors during job execution");
            }
        } catch (Exception e) {
            log.error("runJob exception，jobName={}， exception={}", jobName, e);
        }
    }

    public static void runTransformation(String filename, Map<String, String> variable) {
        try {
            KettleEnvironment.init();
            TransMeta transMeta = new TransMeta(filename);
            Trans trans = new Trans(transMeta);
            variable.forEach((key, value) -> {
                trans.setVariable(key, value);
            });
            trans.execute(new String[0]);
            trans.waitUntilFinished();
            if (trans.getErrors() > 0) {
                throw new RuntimeException("There were errors during transformation execution");
            }
        } catch (KettleException e) {
            log.error("runTransformation exception, jobName={}， exception={}", filename, e);
        }
    }

    public static void main(String[] args) throws URISyntaxException {
        URL url = KettleTools.class.getResource("/kettle/kettle-job-demo.kjb");
        runJob(new File(url.toURI()).getAbsolutePath(), new HashMap<>());
    }
}
