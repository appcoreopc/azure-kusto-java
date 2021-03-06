package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.result.IngestionStatusInTableDescription;

import java.util.Map;
import java.util.UUID;

final public class IngestionBlobInfo{
    public String blobPath;
    public Long rawDataSize;
    public String databaseName;
    public String tableName;
    public UUID id;
    public Boolean retainBlobOnSuccess;
    public IngestionProperties.IngestionReportLevel reportLevel;
    public IngestionProperties.IngestionReportMethod reportMethod;
    public Boolean flushImmediately;
    public IngestionStatusInTableDescription IngestionStatusInTable;

    public Map<String, String> additionalProperties;

    public IngestionBlobInfo(String blobPath, String databaseName, String tableName) {
        this.blobPath = blobPath;
        this.databaseName = databaseName;
        this.tableName = tableName;
        id = UUID.randomUUID();
        retainBlobOnSuccess = true;
        flushImmediately = false;
        reportLevel = IngestionProperties.IngestionReportLevel.FailuresOnly;
        reportMethod = IngestionProperties.IngestionReportMethod.Queue;
    }
}
