package org.example.gcp.cloudtask;

import com.google.cloud.tasks.v2.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.springframework.stereotype.Service;

import java.nio.charset.Charset;
import java.time.Clock;
import java.time.Instant;

@Service
public class CreateTask {
    public void addTask() throws Exception {
        // Instantiates a client.
        try (CloudTasksClient client = CloudTasksClient.create()) {
            // Variables provided by system variables.
            String projectId = System.getenv("GOOGLE_CLOUD_PROJECT");
            String queueName = System.getenv("default");
            String location = System.getenv("europe-west1");
            // Optional variables.
            String payload = "hello";
            int seconds = 5; // Scheduled delay for the task in seconds

            // Construct the fully qualified queue name.
            String queuePath = QueueName.of(projectId, location, queueName).toString();

            // Construct the task body.
            Task.Builder taskBuilder =
                    Task.newBuilder()
                            .setAppEngineHttpRequest(
                                    AppEngineHttpRequest.newBuilder()
                                            .setBody(ByteString.copyFrom(payload, Charset.defaultCharset()))
                                            .setRelativeUri("/remind")
                                            .setHttpMethod(HttpMethod.GET)
                                            .build());

            // Add the scheduled time to the request.
            taskBuilder.setScheduleTime(
                    Timestamp.newBuilder()
                            .setSeconds(Instant.now(Clock.systemUTC()).plusSeconds(seconds).getEpochSecond()));

            // Send create task request.
            Task task = client.createTask(queuePath, taskBuilder.build());
            System.out.println("Task created: " + task.getName());
        }
    }

    public void addTaskForReadingFromGCS() throws Exception {
        // Instantiates a client.
        try (CloudTasksClient client = CloudTasksClient.create()) {
            // Variables provided by system variables.
            String projectId = "GOOGLE_CLOUD_PROJECT";
            String queueName = "default";
            String location = "europe-west1";
            // Optional variables.
            String payload = "hello";
            int seconds = 1; // Scheduled delay for the task in seconds

            // Construct the fully qualified queue name.
            String queuePath = QueueName.of(projectId, location, queueName).toString();

            // Construct the task body.
            Task.Builder taskBuilder =
                    Task.newBuilder()
                            .setAppEngineHttpRequest(
                                    AppEngineHttpRequest.newBuilder()
//                                            .setBody(ByteString.copyFrom(payload, Charset.defaultCharset()))
                                            .setRelativeUri("/setDataFromCsv")
                                            .setHttpMethod(HttpMethod.GET)
                                            .build());


            // Add the scheduled time to the request.
            taskBuilder.setScheduleTime(
                    Timestamp.newBuilder()
                            .setSeconds(Instant.now(Clock.systemUTC()).plusSeconds(seconds).getEpochSecond()));

            // Send create task request.
            Task task = client.createTask(queuePath, taskBuilder.build());
            System.out.println("Task created: " + task.getName());
        }
    }
}
