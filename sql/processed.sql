DROP TABLE IF EXISTS Processed_Files;
CREATE TABLE Processed_Files (
    file_name TEXT PRIMARY KEY,
    processed_timestamp DATETIME
);