package com.gurudatt.flink;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * This plugin is used to upload the job jar to the Apache flink jopb
 */


@Mojo(name = "runjob", requiresOnline = true)
public class FlinkJobUploadMojo extends AbstractMojo {
    @Parameter( property = "runjob.jobmanagerip", defaultValue = "localhost:8081")
    private String jobmanagerip;
    @Parameter( property = "runjob.allowNonRestoredState" ,defaultValue = "false")
    private Boolean allowNonRestoredState;
    @Parameter (property = "runjob.entryClass")
    private String entryClass;
    @Parameter(property = "runjob.parallelism", defaultValue = "1")
    private Integer parallelism;
    @Parameter(property = "runjob.programArgs")
    private String programArgs;
    @Parameter(property = "runjob.savepointPath")
    private String savePointPath;
    @Parameter(property = "runjob.jarPath",required = true)
    private String jarPath;

    private String JARS_BASE_URL="/jars";
    private String JOB_UPLOAD_URL_FRAGMENT="/upload";
    private String JOB_RUN_URL_FRAGMENT="/run";

    public void execute() throws MojoExecutionException, MojoFailureException {
       getLog().info("flink-upload-plugin: "+jarPath);

       File fileToUpload = new File(jarPath);
       String requestUrl = "http://"+jobmanagerip+JARS_BASE_URL+JOB_UPLOAD_URL_FRAGMENT;
        System.out.println(requestUrl);
        try {
            MultipartUtility multipartUtility = new MultipartUtility(requestUrl,"UTF-8");
            multipartUtility.addFilePart("jarfile",fileToUpload);
            List<String> response = multipartUtility.finish();
            System.out.println("SERVER REPLIED: ");

            for (String line: response){
                System.out.println(line);
            }
        } catch (IOException e) {
            System.err.println(e);
        }

    }


}
