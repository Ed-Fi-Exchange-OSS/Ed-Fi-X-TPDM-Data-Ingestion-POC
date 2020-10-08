# Banner Connector POC

This application is a proof of concept to show capabilities of connecting to a 
Banner database and using SQL retrieve the required data for populating the 
TPDM APIs.

## Files and Folders

### Files

`README` - Introduces and explains the application  
`banner-connector-1.0.jar` - This is the java executable that will perform the work  
`input/application.properties` - This file is where the properties are to be set  
`run.sh` - A shell script used to run the application

### Folders

`input/columnmap` - Contains mapping files for mapping banner columns to TPDM
columns e.g. teacherCandidateId=SPRIDEN_PIDM  
`input/sql` - Contains SQL files for retrieving the necessary Banner data  
`lib` - Contains the library files needed to run the application  
`output` - This is where the report is written

## Configuration Steps

- Set the properties in `input/application.properties`
- Adjust provided SQL in the `input/sql` folder
- Adjust the mappings, if necessary, in the `input/columnmap` folder

## Executing the Application

To run the application, execute the following from a terminal/command window.  
`java -jar .\banner-connector-1.0.jar --spring.config.location=file:///data/input/application.properties`  

For Linux, a shell script has been provided called `run.sh`.  You may need to make it and
executable file first by running `chmod +x run.sh`.  

For Windows, a bat file has been provided called `run.bat`.