# Azure Blob Storage Writer

The purpose of Azure Blob Storage Writer is to export any CSV files into the destinated Blob containers.

## API documentation

[Azure Blob Storage](https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob?view=azure-python-previous)

## Limitations

- Each block has a maximum of 100 MB
- A block blob can include up to 50,000 blocks
- Maximum size of a block blib is 4.75 TB

## Constraints

If SAS token is used, the componet will be skipping the container validation process. The reason is SAS token is configured/generated with a targeted blob object, meaning that it has limited access rights to one Blob only.

## Configuration

### Input Mapping Configurations
The component will be using the input mapping to determine what files are needed to export into the configured Azure Blob Storage. File name configuration in the inputting namming will be used as a naming convention in the Blob storage container.

    Input Mapping
        Source      in.c-main.kbc_export
        File Name   kbc_export.csv

In the above configuration, File Name (kbc_export.csv) will be used to export into the Blob storage container.
    

### Component Parameters Configurations
1. Account Name - `Required`
    - Azure storage account name
2. Account Key
    - `Required` unless [SAS Token] is specified
    - Azure storage account access key
    - If Account key is present, it will be used over the SAS token
    - Account Key can be found:
      ```
      [Your Storage Account Overview] > Settings > Access Keys
      ```
3. SAS Token
    - `Required` unless [Account Key] is specified
    - Shared Access Signature generated from Azure storage targeted to a Blob
4. Container Name - `Required`
    - Azure storage container name
5. Destination Path
    - The folder path you wish to export the input files to
    - Backslash '/' represents a folder path and it needs to be included at the end of the folder names.
    - Please leave this blank if you wish to export the files into the root folder of the container
    - Example: 
        - To export into the [test] folder, enter
          ```
          test/
          ```
6. Append Date to File
    - Appending dates into the end of the export file name
    - [Today]'s date in format (YYYY_MM_dd) will be appended into the exported file
    - Example:
        - Input mapping file name:
          ```
          test.csv
          ```
        - Exported Blob storage name:
          ```
          test_2020_01_01.csv
          ```

