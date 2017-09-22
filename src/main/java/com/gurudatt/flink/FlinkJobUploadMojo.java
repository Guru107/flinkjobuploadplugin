package com.gurudatt.flink;

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


        String requestUrl = "http://"+jobmanagerip+JARS_BASE_URL+JOB_UPLOAD_URL_FRAGMENT;

        CloseableHttpClient httpClient = HttpClients.createDefault();

        HttpPost uploadFile = new HttpPost(requestUrl);
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        File fileToUpload = new File(jarPath);

        builder.addBinaryBody("jarfile",fileToUpload);

        HttpEntity multipart = builder.build();

        uploadFile.setEntity(multipart);
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(uploadFile);
            response.setHeader(new BasicHeader("Expect",""));

        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            String bodyAsString = EntityUtils.toString(response.getEntity(),"UTF-8");
            JSONObject jsonObject = new JSONObject(bodyAsString);
            System.out.println(jsonObject.get("filename"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
