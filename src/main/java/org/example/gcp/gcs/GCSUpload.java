package org.example.gcp.gcs;

import com.google.cloud.storage.*;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;

@Service
public class GCSUpload {

    public void uploadFile(String projectId, String bucketName, String objectName, byte[] fileContents, String contentType) {
        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
        BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(bucketName, objectName)).setContentType(contentType).build();
        try {
            storage.createFrom(blobInfo,new ByteArrayInputStream(fileContents));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

