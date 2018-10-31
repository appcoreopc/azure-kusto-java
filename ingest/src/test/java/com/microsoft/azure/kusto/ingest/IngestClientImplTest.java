package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

class IngestClientImplTest {

    private static ResourceManager resourceManagerMock = mock(ResourceManager.class);
    private static IngestClient ingestClientMock = mock(IngestClient.class);
    private static IngestClientImpl ingestClientImplMock = mock(IngestClientImpl.class);
    private static IngestionProperties props;
    private static IngestClient ingestClient;

    @BeforeAll
    static void setUp() {
        try {
            when(resourceManagerMock.getIngestionResource(ResourceManager.ResourceType.SECURED_READY_FOR_AGGREGATION_QUEUE))
                    .thenReturn("queue1")
                    .thenReturn("queue2");

            when(resourceManagerMock.getIngestionResource(ResourceManager.ResourceType.TEMP_STORAGE))
                    .thenReturn("storage1")
                    .thenReturn("storage2");

            when(resourceManagerMock.getIngestionResource(ResourceManager.ResourceType.INGESTIONS_STATUS_TABLE))
                    .thenReturn("statusTable");

            when(resourceManagerMock.getIdentityToken())
                    .thenReturn("identityToken");

            props = new IngestionProperties("dbName", "tableName");
            props.setJsonMappingName("mappingName");

            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials("resource.uri", "client-id", "applicationKey", "authority-id");
            ingestClient = IngestClientFactory.createClient(csb);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void ingestFromBlob() {
        try {
            BlobSourceInfo blobSourceInfo = new BlobSourceInfo("blobPath", 100);
            ingestClientMock.ingestFromBlob(blobSourceInfo, props);

            verify(ingestClientMock).ingestFromBlob(any(BlobSourceInfo.class), any(IngestionProperties.class));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void ingestFromBlobThrowExceptionWhenArgumentIsNull() {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo("blob.path", 100);

        assertThrows(IllegalArgumentException.class,
                () -> ingestClient.ingestFromBlob(null, null));

        assertThrows(IllegalArgumentException.class,
                () -> ingestClient.ingestFromBlob(blobSourceInfo, null));

        assertThrows(IllegalArgumentException.class,
                () -> ingestClient.ingestFromBlob(null, props));
    }

    @Test
    void ingestFromFile() {
        try {
            String testFilePath = Paths.get("src", "test", "resources", "testdata.json").toString();
            when(ingestClientImplMock.uploadLocalFileToBlob(isA(String.class), isA(String.class), isA(String.class)))
                    .thenReturn(new CloudBlockBlob(new URI("https://ms.com/storageUri")));

            doNothing().when(ingestClientImplMock).postMessageToQueue(isA(String.class), isA(String.class));

            FileSourceInfo fileSourceInfo = new FileSourceInfo(testFilePath, 0);
            int numOfFiles = 3;
            for (int i = 0; i < numOfFiles; i++) {
                ingestClientMock.ingestFromFile(fileSourceInfo, props);
            }

            verify(ingestClientMock, times(numOfFiles)).ingestFromFile(any(FileSourceInfo.class), any(IngestionProperties.class));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void ingestFromFileThrowExceptionWhenArgumentIsNull() {
        FileSourceInfo fileSourceInfo = new FileSourceInfo("file.path", 100);

        assertThrows(IllegalArgumentException.class,
                () -> ingestClient.ingestFromFile(null, null));

        assertThrows(IllegalArgumentException.class,
                () -> ingestClient.ingestFromFile(fileSourceInfo, null));

        assertThrows(IllegalArgumentException.class,
                () -> ingestClient.ingestFromFile(null, props));
    }

    @Test
    void ingestFromFileThrowExceptionWhenFileDoesNotExist() {
        FileSourceInfo fileSourceInfo = new FileSourceInfo("file.path", 100);

        assertThrows(IllegalArgumentException.class,
                () -> ingestClient.ingestFromFile(fileSourceInfo, props));
    }

    @Test
    void ingestFromStream() {
        try {
            String testFilePath = Paths.get("src", "test", "resources", "testdata.json").toString();
            when(ingestClientImplMock.uploadStreamToBlob(isA(InputStream.class), isA(String.class), isA(String.class), isA(Boolean.class)))
                    .thenReturn(new CloudBlockBlob(new URI("https://ms.com/storageUri")));
            doNothing().when(ingestClientImplMock).postMessageToQueue(isA(String.class), isA(String.class));
            int numOfFiles = 3;
            for (int i = 0; i < numOfFiles; i++) {
                InputStream stream = new FileInputStream(testFilePath);
                StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream,false);
                ingestClientMock.ingestFromStream(streamSourceInfo, props);
            }
            verify(ingestClientMock, times(numOfFiles)).ingestFromStream(any(StreamSourceInfo.class), any(IngestionProperties.class));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void ingestFromStreamThrowExceptionWhenArgumentIsNull() {
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(null);

        assertThrows(IllegalArgumentException.class,
                () -> ingestClient.ingestFromStream(null, null));

        assertThrows(IllegalArgumentException.class,
                () -> ingestClient.ingestFromStream(streamSourceInfo, null));

        assertThrows(IllegalArgumentException.class,
                () -> ingestClient.ingestFromStream(null, props));

    }
}
