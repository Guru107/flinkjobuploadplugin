# flinkjobuploadplugin
This will automate the task of uploading the fat jar to flink cluster right from your IDE (If you have proper access to the cluster).

This maven plugin can be used to automate the process of job running. 
Just add this in `pom.xml` file

Default Configurations



````
<build>
    <plugins>
        ...
        <plugin>
            <groupId>com.github.guru107</groupId>
            <artifactId>flinkjobupload-maven-plugin</artifactId>
            <version>1.0</version>
            <configuration>
                <jobmanagerip>localhost:8081</jobmanagerip>
                <jarPath>${project.build.directory}/${finalName}.jar</jarPath>
                <allowNonRestoredState>false</allowNonRestoredState>
                <entryClass></entryClass>
                <parallelism>1</parallelism>
                <programArgs></programArgs>
                <savepointPath></savepointPath>
            </configuration>
            <executions>
                <execution>
                    <goals>
                        <goal>runjob</goal>
                    </goals>
                </execution>
            </executions>
        </plugin>
        ...
    </plugins>
</build>

 ````
