# Azure Blob Storage Writer

The purpose of Azure Blob Storage Writer is to export any CSV files into the destinated Blob containers.

## Limitations

- Each block has a maximum of 100 MB
- A block blob can include up to 50,000 blocks
- Maximum size of a block blib is 4.75 TB

## Token Permission Requirement
    Allowed services: 
        1. Blob
    Allowed resource types:
        1. Service
        2. Container
        3. Object
    Allowed Permissions:
        1. Read
        2. List

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
2. Account Key - `Required`
    - Azure storage account access key
    - Account Key can be found:
      ```
      [Your Storage Account Overview] > Settings > Access Keys
      ```
3. Container Name - `Required`
    - Azure storage container name
4. Destination Path
    - The folder path you wish to export the input files to
    - Please leave this blank if you wish to export the files into the root folder of the container
    - If slash '/' is not included at the end of the path configuration, the component will automatically add the '/' for you. Slash represents every folder/directory the component has to pass through in the blob storage directory.
    - Example: 
        - To export into the [test] folder, enter
          ```
          test/
          ```
5. Append Date to File
    - Appending dates into the end of the export file name
    - [Today]'s date in format (YYYY_MM_dd) will be appended into the exported file
    - Example:
        - Input mapping file name:
          ```
          test.csv
          ```
        - Exported Blob storage name:
          ```
          test-2020_01_01.csv
          ```

