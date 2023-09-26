bring cloud;

interface Storage {
    inflight put(key: str, contents: str);
    inflight get(key: str): str;

    onCreate(callback: inflight (str): void);
}

class BucketStorage impl Storage {
    bucket: cloud.Bucket;

    init() {
        this.bucket = new cloud.Bucket();
    }

    pub inflight put(key: str, contents: str) {
        this.bucket.put(key, contents);
    }

    pub inflight get(key: str): str {
        return this.bucket.get(key);
    }

    pub onCreate(callback: inflight (str): void) {
        this.bucket.onCreate(inflight (key) => {
            callback(key);
        });
    }
}

bring ex;

class TableStorage impl Storage {
    table: ex.Table;
    topic: cloud.Topic;

    init() {
        this.table = new ex.Table(
            name: "files",
            primaryKey: "key",
            columns: {
                key: ex.ColumnType.STRING,
                contents: ex.ColumnType.STRING,
            }
        );

        this.topic = new cloud.Topic();
    }

    pub inflight put(key: str, contents: str) {
        this.table.insert(key, {
            key: key,
            contents: contents,
        });
        this.topic.publish(key);
    }

    pub inflight get(key: str): str {
        return this.table.get(key).get("contents").asStr();
    }

    pub onCreate(callback: inflight (str): void) {
        this.topic.onMessage(inflight (key) => {
            callback(key);
        });
    }
}

class FileProcessor {
    uploads: Storage;
    processed: Storage;

    init(uploads: Storage, processed: Storage) {
        this.uploads = uploads;
        this.processed = processed;

        this.uploads.onCreate(inflight (key) => {
            let contents = this.uploads.get(key);
            let uppercase = contents.uppercase();
            this.processed.put(key, uppercase);
        });
    }

    pub inflight put(key: str, contents: str) {
        this.uploads.put(key, contents);
    }

    pub inflight get(key: str): str {
        return this.processed.get(key);
    }

    pub onFileProcessed(callback: inflight (str): void) {
        this.processed.onCreate(inflight (key) => {
            callback(key);
        });
    }
}

let processor = new FileProcessor(
    new TableStorage() as "uploads",
    new BucketStorage() as "processed",
);

processor.onFileProcessed(inflight (key) => {
    log("file ${key} processed: ${processor.get(key)}");
});

new cloud.Function(inflight () => {
    processor.put("hello", "world");
});
