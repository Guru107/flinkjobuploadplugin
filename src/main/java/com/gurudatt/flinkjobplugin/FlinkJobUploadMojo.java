package com.gurudatt.flinkjobplugin;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;


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
    private String entryClass = "";
    @Parameter(property = "runjob.parallelism", defaultValue = "1")
    private Integer parallelism;
    @Parameter(property = "runjob.programArgs")
    private String programArgs = "";
    @Parameter(property = "runjob.savepointPath")
    private String savePointPath = "";
    @Parameter(property = "runjob.jarPath",required = true)
    private String jarPath;

    private String JARS_BASE_URL="/jars";
    private String JOB_UPLOAD_URL_FRAGMENT="/upload";
    private String JOB_RUN_URL_FRAGMENT="/run";

    public void execute() throws MojoExecutionException, MojoFailureException {
        getLog().info("flink-upload-plugin: "+jarPath);


        String jobUploadUrl = "http://"+jobmanagerip+JARS_BASE_URL+JOB_UPLOAD_URL_FRAGMENT;
        getLog().info("JOB_UPLOAD_URL: "+jobUploadUrl);

        JSONObject uploadResponse = uploadJob(jobUploadUrl,jarPath);
        if(uploadResponse != null
                && !uploadResponse.has("error")
                && uploadResponse.getString("status").equals("success")) {

            String uploadedJarName = uploadResponse.getString("filename");

            String runJobUrl = "http://" +jobmanagerip + JARS_BASE_URL + "/" + uploadedJarName + JOB_RUN_URL_FRAGMENT
                    +"?allowNonRestoredState="+allowNonRestoredState
                    +"&entry-class="+entryClass
                    +"&parallelism="+parallelism
                    +"&program-args="+programArgs
                    +"&savepointPath="+savePointPath;

            getLog().info("RUN_JOB_URL: "+runJobUrl);
            JSONObject runResponse = runJob(runJobUrl);
            if(runResponse != null && !runResponse.has("error")){
                getLog().info("JOB_ID: "+runResponse.getString("jobid"));
            }else{
                throw new MojoFailureException("RunJobResponseFailure");
            }


        } else {
            throw new MojoFailureException("UploadJobResponseFailure");
        }



    }

    /**
     *
     * @param requestUrl - JobManager job upload url
     * @param jarPath - fat-jar complete path
     * @return JSONObject
     * @throws MojoFailureException
     */

    private JSONObject uploadJob(String requestUrl, String jarPath) throws MojoFailureException {

        CloseableHttpClient httpClient = HttpClients.createDefault();
        File fileToUpload = new File(jarPath);
        HttpPost uploadFileUrl = new HttpPost(requestUrl);

        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.addBinaryBody("jarfile",fileToUpload);

        HttpEntity multipart = builder.build();

        JSONObject jobUploadResponse = null;



        uploadFileUrl.setEntity(multipart);
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(uploadFileUrl);
            response.setHeader(new BasicHeader("Expect",""));
            String bodyAsString = EntityUtils.toString(response.getEntity(),"UTF-8");
            jobUploadResponse = new JSONObject(bodyAsString);
            getLog().info(bodyAsString);
        } catch (IOException e) {
            throw new MojoFailureException("UploadJobMethod",e);
        }finally {
            try {
                response.close();
                httpClient.close();
            } catch (IOException e) {
                throw new MojoFailureException("UploadJobMethod",e);
            }
        }

        return jobUploadResponse;
    }

    private JSONObject runJob(String uploadedJarUrl) throws MojoFailureException {

        JSONObject runJobResponse = null;
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost runJobUrl = new HttpPost(uploadedJarUrl);

        CloseableHttpResponse response = null;

        try {
            response = httpClient.execute(runJobUrl);
            String bodyAsString = EntityUtils.toString(response.getEntity(),"UTF-8");
            runJobResponse = new JSONObject(bodyAsString);
            getLog().info(bodyAsString);
        } catch (IOException e) {
            e.printStackTrace();
            throw new MojoFailureException("RunJobMethod",e);
        }finally {
            try {
                response.close();
                httpClient.close();
            } catch (IOException e) {
                throw new MojoFailureException("RunJobMethod",e);
            }
        }

        return runJobResponse;
    }

}
