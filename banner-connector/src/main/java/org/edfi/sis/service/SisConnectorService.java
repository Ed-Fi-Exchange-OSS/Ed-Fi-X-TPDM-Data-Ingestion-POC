package org.edfi.sis.service;

import com.opencsv.RFC4180Parser;
import org.apache.commons.lang3.StringUtils;
import org.edfi.api.ApiClient;
import org.edfi.api.ApiException;
import org.edfi.api.descriptor.AcademicSubjectDescriptorsApi;
import org.edfi.api.descriptor.GradeLevelDescriptorsApi;
import org.edfi.api.descriptor.SexDescriptorsApi;
import org.edfi.api.descriptor.TppDegreeTypeDescriptorsApi;
import org.edfi.api.resource.TeacherCandidatesApi;
import org.edfi.model.descriptor.EdFiAcademicSubjectDescriptor;
import org.edfi.model.descriptor.EdFiGradeLevelDescriptor;
import org.edfi.model.descriptor.EdFiSexDescriptor;
import org.edfi.model.descriptor.TpdmTppDegreeTypeDescriptor;
import org.edfi.model.resource.EdFiStudentReference;
import org.edfi.model.resource.TpdmTeacherCandidate;
import org.edfi.model.resource.TpdmTeacherCandidateTPPProgramDegree;
import org.edfi.sis.api.TokenRetriever;
import org.edfi.sis.dao.Dao;
import org.edfi.sis.model.SisConnectorResponse;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import javax.naming.AuthenticationException;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Stream;

@Component
public class SisConnectorService {
    @Autowired
    Dao dao;

    @Autowired
    TokenRetriever tokenRetriever;

    @Autowired
    ApiClient apiClient;

    @Value( "${input.sql.dir}" )
    String sqlDirectory;

    @Value( "${input.columnmap.dir}" )
    String columnMapDirectory;

    @Value( "${output.dir}" )
    String outputDirectory;

    public static final String TEACHER_CANDIDATE_SQL_NAME = "teacherCandidate";

    Map<String, EdFiAcademicSubjectDescriptor> academicSubjectDescriptorMap = null;
    Map<String, EdFiGradeLevelDescriptor> gradeLevelDescriptorMap = null;
    Map<String, TpdmTppDegreeTypeDescriptor> tppDegreeTypeDescriptorMap = null;
    Map<String, EdFiSexDescriptor> sexDescriptorMap = null;
    Map<String, String> existingTeacherCandidateMap = null;
    Map<String, Map<String, String>> columnsMap = null;
    Map<String, String> sqlMap = null;

    private final RFC4180Parser rfc4180Parser = new RFC4180Parser();

    public SisConnectorService(Dao dao) {
        this.dao = dao;
    }

    public void handleRequest() {
        SisConnectorResponse response = new SisConnectorResponse();
        DateTime startTime = new DateTime();
        response.setStartTime(startTime);
        try {
            apiClient.setAccessToken(tokenRetriever.obtainNewBearerToken());

            initializeMaps();

            dao.getRemoteConnection();

            processTeacherCandidate(response);

            removeDeletedTeacherCandidates(response);

        } catch (AuthenticationException | ApiException e) {
            response.setFatalError(true);
            response.setErrorMessage(e.getMessage());
            response.setException(e);
        } catch (SQLException e) {
            response.setFatalError(true);
            response.setErrorMessage(e.getMessage());
            response.setException(e);
        } catch (ClassNotFoundException e) {
            response.setFatalError(true);
            response.setErrorMessage(e.getMessage());
            response.setException(e);
        } finally {
            try {
                dao.closeRemoteConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        DateTime endTime = new DateTime();
        response.setEndTime(endTime);
        response.setDuration(endTime.getMillis() - startTime.getMillis());
        buildReport(response);
    }

    private void initializeMaps() throws AuthenticationException, ApiException {
        sqlMap = loadSqlMap();
        academicSubjectDescriptorMap = loadAcademicSubjectDescriptorsMap();
        gradeLevelDescriptorMap = loadGradeLevelDescriptorsMap();
        tppDegreeTypeDescriptorMap = loadTppDegreeTypeDescriptorsMap();
        sexDescriptorMap = loadSexDescriptorsMap();
        columnsMap = loadColumnsMap();
        existingTeacherCandidateMap = loadExistingTeacherCandidateMap();
    }

    private void processTeacherCandidate(SisConnectorResponse response) throws AuthenticationException, ApiException {
        boolean firstRow = true;
        int teacherCandidateIdIndex = 0;
        int firstNameIndex = 0;
        int middleNameIndex = 0;
        int lastNameIndex = 0;
        int birthDateIndex = 0;
        int academicSubjectDescriptorIndex = 0;
        int gradeLevelDescriptorIndex = 0;
        int tppDegreeTypeDescriptorIndex = 0;
        int studentUniqueIdIndex = 0;
        int sexIndex = 0;
        int upsertCount = 0;

        List<List<String>> result = dao.makeSqlCall(sqlMap.get(TEACHER_CANDIDATE_SQL_NAME));
        for (List<String> row : result) {
            if (firstRow) {
                int currentIndex = 0;
                for (String header : row) {
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_SQL_NAME).get("teacherCandidateId"))) {
                        teacherCandidateIdIndex = currentIndex;
                    } else if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_SQL_NAME).get("firstName"))) {
                        firstNameIndex = currentIndex;
                    } else if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_SQL_NAME).get("lastName"))) {
                        lastNameIndex = currentIndex;
                    } else if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_SQL_NAME).get("birthDate"))) {
                        birthDateIndex = currentIndex;
                    } else if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_SQL_NAME).get("academicSubjectDescriptor"))) {
                        academicSubjectDescriptorIndex = currentIndex;
                    } else if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_SQL_NAME).get("gradeLevelDescriptor"))) {
                        gradeLevelDescriptorIndex = currentIndex;
                    } else if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_SQL_NAME).get("tppDegreeTypeDescriptor"))) {
                        tppDegreeTypeDescriptorIndex = currentIndex;
                    } else if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_SQL_NAME).get("studentUniqueId"))) {
                        studentUniqueIdIndex = currentIndex;
                    } else if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get("teacherCandidate").get("sex"))) {
                        sexIndex = currentIndex;
                    }
                    currentIndex++;
                }
                firstRow = false;
            } else {
                TpdmTeacherCandidate teacherCandidate = createTeacherCandidate(row.get(teacherCandidateIdIndex), row.get(firstNameIndex),
                        row.get(lastNameIndex), row.get(birthDateIndex), row.get(academicSubjectDescriptorIndex),
                        row.get(gradeLevelDescriptorIndex), row.get(tppDegreeTypeDescriptorIndex),
                        row.get(studentUniqueIdIndex), row.get(sexIndex));
                try {
                    saveTeacherCandidate(teacherCandidate);
                    existingTeacherCandidateMap.remove(teacherCandidate.getTeacherCandidateIdentifier());
                    upsertCount++;
                } catch (ApiException ae) {
                    response.addError(teacherCandidate.toString() + String.format("%n") + ae.getResponseBody());
                }
            }
        }
        response.setUpsertCount(upsertCount);
    }

    private void saveTeacherCandidate(TpdmTeacherCandidate teacherCandidate) throws AuthenticationException, ApiException {
        try {
            TeacherCandidatesApi teacherCandidatesApi = new TeacherCandidatesApi(apiClient);
            teacherCandidatesApi.postTeacherCandidate(teacherCandidate);
            existingTeacherCandidateMap.remove(teacherCandidate.getTeacherCandidateIdentifier());
        } catch (ApiException ae) {
            if (ae.getCode()==(HttpStatus.UNAUTHORIZED.value())) {
                apiClient.setAccessToken(tokenRetriever.obtainNewBearerToken());
                TeacherCandidatesApi teacherCandidatesApi = new TeacherCandidatesApi(apiClient);
                teacherCandidatesApi.postTeacherCandidate(teacherCandidate);
            } else {
                throw ae;
            }
        }
    }

    private TpdmTeacherCandidate createTeacherCandidate(String teacherCandidateId, String firstName, String lastName, String birthDate,
                                                        String academicSubject, String gradeLevel, String degreeType,
                                                        String studentId, String sex) {
        TpdmTeacherCandidate teacherCandidate = new TpdmTeacherCandidate();
        teacherCandidate.setTeacherCandidateIdentifier(teacherCandidateId);
        teacherCandidate.setFirstName(firstName);
        teacherCandidate.setLastSurname(lastName);
        teacherCandidate.setBirthDate(LocalDate.parse(birthDate));

        TpdmTeacherCandidateTPPProgramDegree degree = new TpdmTeacherCandidateTPPProgramDegree();
        degree.setAcademicSubjectDescriptor(getAcademicSubjectDescriptorUri(academicSubject));
        degree.setGradeLevelDescriptor(getGradeLevelDescriptorUri(gradeLevel));
        degree.setTppDegreeTypeDescriptor(getTppDegreeTypeDescriptorUri(degreeType));
        List<TpdmTeacherCandidateTPPProgramDegree> degrees = new ArrayList<>();
        degrees.add(degree);
        teacherCandidate.setTppProgramDegrees(degrees);

        EdFiStudentReference studentReference = new EdFiStudentReference();
        studentReference.setStudentUniqueId(studentId);
        teacherCandidate.setStudentReference(studentReference);
        teacherCandidate.setSexDescriptor(getSexDescriptorUri(sex));
        return teacherCandidate;
    }

    private void removeDeletedTeacherCandidates(SisConnectorResponse response)  throws ApiException, AuthenticationException {
        int deleteCount = 0;
        for (String key : existingTeacherCandidateMap.keySet()) {
            deleteTeacherCandidate(existingTeacherCandidateMap.get(key));
            deleteCount++;
        }
        response.setDeleteCount(deleteCount);
    }

    private void deleteTeacherCandidate(String id) throws ApiException, AuthenticationException {
        try {
            TeacherCandidatesApi teacherCandidatesApi = new TeacherCandidatesApi(apiClient);
            teacherCandidatesApi.deleteTeacherCandidateById(id, null);
        } catch (ApiException ae) {
            if (ae.getCode() == (HttpStatus.UNAUTHORIZED.value())) {
                apiClient.setAccessToken(tokenRetriever.obtainNewBearerToken());
                TeacherCandidatesApi teacherCandidatesApi = new TeacherCandidatesApi(apiClient);
                teacherCandidatesApi.deleteTeacherCandidateById(id, null);
            } else {
                throw ae;
            }
        }
    }

    private Map<String, EdFiAcademicSubjectDescriptor> loadAcademicSubjectDescriptorsMap() throws ApiException, AuthenticationException {
        List<EdFiAcademicSubjectDescriptor> list = null;
        try {
            AcademicSubjectDescriptorsApi academicSubjectDescriptorsApi = new AcademicSubjectDescriptorsApi(apiClient);
            list = academicSubjectDescriptorsApi.getAcademicSubjectDescriptors(0, 100, false, null);
        } catch (ApiException ae) {
            if (ae.getCode() == (HttpStatus.UNAUTHORIZED.value())) {
                apiClient.setAccessToken(tokenRetriever.obtainNewBearerToken());
                AcademicSubjectDescriptorsApi academicSubjectDescriptorsApi = new AcademicSubjectDescriptorsApi(apiClient);
                list = academicSubjectDescriptorsApi.getAcademicSubjectDescriptors(0, 100, false, null);
            } else {
                throw ae;
            }
        }
        Map<String, EdFiAcademicSubjectDescriptor> map = new HashMap<>();
        for (EdFiAcademicSubjectDescriptor academicSubjectDescriptor : list) {
            map.put(academicSubjectDescriptor.getCodeValue(), academicSubjectDescriptor);
        }
        return map;
    }
    private String getAcademicSubjectDescriptorUri (String code) {
        EdFiAcademicSubjectDescriptor academicSubjectDescriptor = academicSubjectDescriptorMap.get(code);
        if (academicSubjectDescriptor!=null) {
            return academicSubjectDescriptor.getNamespace() + "#" + code;
        }
        return code;
    }

    private Map<String, EdFiGradeLevelDescriptor> loadGradeLevelDescriptorsMap() throws ApiException, AuthenticationException {
        List<EdFiGradeLevelDescriptor> list = null;
        try {
            GradeLevelDescriptorsApi gradeLevelDescriptorsApi = new GradeLevelDescriptorsApi(apiClient);
            list = gradeLevelDescriptorsApi.getGradeLevelDescriptors(0, 100, false, null);
        } catch (ApiException ae) {
            if (ae.getCode() == (HttpStatus.UNAUTHORIZED.value())) {
                apiClient.setAccessToken(tokenRetriever.obtainNewBearerToken());
                GradeLevelDescriptorsApi gradeLevelDescriptorsApi = new GradeLevelDescriptorsApi(apiClient);
                list = gradeLevelDescriptorsApi.getGradeLevelDescriptors(0, 100, false, null);
            } else {
                throw ae;
            }
        }
        Map<String, EdFiGradeLevelDescriptor> map = new HashMap<>();
        for (EdFiGradeLevelDescriptor gradeLevelDescriptor : list) {
            map.put(gradeLevelDescriptor.getCodeValue(), gradeLevelDescriptor);
        }
        return map;
    }
    private String getGradeLevelDescriptorUri (String code) {
        EdFiGradeLevelDescriptor gradeLevelDescriptor = gradeLevelDescriptorMap.get(code);
        if (gradeLevelDescriptor!=null) {
            return gradeLevelDescriptor.getNamespace() + "#" + code;
        }
        return code;
    }

    private Map<String, TpdmTppDegreeTypeDescriptor> loadTppDegreeTypeDescriptorsMap() throws ApiException, AuthenticationException {
        List<TpdmTppDegreeTypeDescriptor> list = null;
        try {
            TppDegreeTypeDescriptorsApi tppDegreeTypeDescriptorsApi = new TppDegreeTypeDescriptorsApi(apiClient);
            list = tppDegreeTypeDescriptorsApi.getTPPDegreeTypeDescriptors(0, 100, false, null);
        } catch (ApiException ae) {
            if (ae.getCode() == (HttpStatus.UNAUTHORIZED.value())) {
                apiClient.setAccessToken(tokenRetriever.obtainNewBearerToken());
                TppDegreeTypeDescriptorsApi tppDegreeTypeDescriptorsApi = new TppDegreeTypeDescriptorsApi(apiClient);
                list = tppDegreeTypeDescriptorsApi.getTPPDegreeTypeDescriptors(0, 100, false, null);
            } else {
                throw ae;
            }
        }
        Map<String, TpdmTppDegreeTypeDescriptor> map = new HashMap<>();
        for (TpdmTppDegreeTypeDescriptor tppDegreeTypeDescriptor : list) {
            map.put(tppDegreeTypeDescriptor.getCodeValue(), tppDegreeTypeDescriptor);
        }
        return map;
    }
    private String getTppDegreeTypeDescriptorUri (String code) {
        TpdmTppDegreeTypeDescriptor tppDegreeTypeDescriptor = tppDegreeTypeDescriptorMap.get(code);
        if (tppDegreeTypeDescriptor!=null) {
            return tppDegreeTypeDescriptor.getNamespace() + "#" + code;
        }
        return code;
    }

    private Map<String, EdFiSexDescriptor> loadSexDescriptorsMap() throws ApiException, AuthenticationException {
        List<EdFiSexDescriptor> list = null;
        try {
            SexDescriptorsApi sexDescriptorsApi = new SexDescriptorsApi(apiClient);
            list = sexDescriptorsApi.getSexDescriptors(0, 100, false, null);
        } catch (ApiException ae) {
            if (ae.getCode() == (HttpStatus.UNAUTHORIZED.value())) {
                apiClient.setAccessToken(tokenRetriever.obtainNewBearerToken());
                SexDescriptorsApi sexDescriptorsApi = new SexDescriptorsApi(apiClient);
                list = sexDescriptorsApi.getSexDescriptors(0, 100, false, null);
            } else {
                throw ae;
            }
        }
        Map<String, EdFiSexDescriptor> map = new HashMap<>();
        for (EdFiSexDescriptor edFiAcademicSubjectDescriptor : list) {
            map.put(edFiAcademicSubjectDescriptor.getCodeValue(), edFiAcademicSubjectDescriptor);
        }
        return map;
    }

    private String getSexDescriptorUri (String code) {
        EdFiSexDescriptor sexDescriptor = sexDescriptorMap.get(code);
        if (sexDescriptor!=null) {
            return sexDescriptor.getNamespace() + "#" + code;
        }
        return code;
    }

    private Map<String, String> loadExistingTeacherCandidateMap() throws AuthenticationException, ApiException {
        List<TpdmTeacherCandidate> list = null;
        try {
            TeacherCandidatesApi teacherCandidatesApi = new TeacherCandidatesApi(apiClient);
            list = teacherCandidatesApi.getTeacherCandidates(0, 100, false, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        } catch (ApiException ae) {
            if (ae.getCode() == (HttpStatus.UNAUTHORIZED.value())) {
                apiClient.setAccessToken(tokenRetriever.obtainNewBearerToken());
                TeacherCandidatesApi teacherCandidatesApi = new TeacherCandidatesApi(apiClient);
                list = teacherCandidatesApi.getTeacherCandidates(0, 100, false, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
            } else {
                throw ae;
            }
        }
        Map<String, String> map = new HashMap<>();
        list.forEach(tc -> map.put(tc.getTeacherCandidateIdentifier(), tc.getId()));

        return map;
    }

    private Map<String, String> loadSqlMap() {
        Map<String, String> sqlMap = new HashMap<>();

        File folder = new File(getSqlDirectory());
        File[] fileList = folder.listFiles();
        for (File file: fileList) {
            String name = parseName(file.getName());
            String sql = readSql(file);
            sqlMap.put(name, sql);
        }
        return sqlMap;
    }

    private Map<String, Map<String, String>> loadColumnsMap () {
        Map<String, Map<String, String>> fullMap = new HashMap<>();
        File folder = new File(getColumnMapDirectory());
        File[] fileList = folder.listFiles();
        for (File file: fileList) {
            String name = parseName(file.getName());
            Map<String, String> map = readColumnMap(file);
            fullMap.put(name, map);
        }
        return fullMap;
    }

    public void buildReport(SisConnectorResponse response) {
        String report = response.buildReport();
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter( new FileWriter(getOutputFileName()));
            writer.write(report);
        }
        catch ( IOException e) {
        } finally {
            try {
                if ( writer != null)
                    writer.close( );
            } catch ( IOException e) {
            }
        }
    }

    private String getOutputFileName () {
        DateTimeFormatter formatDate = DateTimeFormat.forPattern("YYYYMMdd");
        DateTime dateTime = DateTime.now();
        DateTimeFormatter formatHour = DateTimeFormat.forPattern("HHmmss");
        return getOutputDirectory() + "/" +  dateTime.toString(formatDate) + "-" + dateTime.toString(formatHour) + ".report";
    }

//    public void process() {
//        try {
//            File folder = new File(getInputDirectory());
//            File[] sqlFileList = folder.listFiles();
//
//            dao.getRemoteConnection();
//
//            for (File sqlFile: sqlFileList) {
//                String name = parseName(sqlFile.getName());
//                String sql = readSql(sqlFile);
//
//                List<List<String>> result = dao.makeSqlCall(sql);
//
//                boolean first = true;
//                File csvFile = new File(this.getOutputDirectory() + "/" +name + ".csv");
//                BufferedWriter bw = new BufferedWriter(new FileWriter(csvFile));
//                for (List<String> row : result) {
//                    String csvRow = rfc4180Parser.parseToLine(row.stream().toArray(String[]::new), true);
//                    if (!first) {
//                        csvRow = "\n" + csvRow;
//                    }
//                    bw.write(csvRow);
//                    first = false;
//                }
//                bw.close();
//            }
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            try {
//                dao.closeRemoteConnection();
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
//    }

    private String parseName(String fullname) {
        return fullname.substring(0, fullname.lastIndexOf('.'));
    }

    private String readSql(File file) {
        StringBuilder contentBuilder = new StringBuilder();
        try (Stream<String> stream = Files.lines( Paths.get(file.getAbsolutePath()), StandardCharsets.UTF_8))
        {
            stream.forEach(s -> contentBuilder.append(s).append(" "));
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return contentBuilder.toString();
    }

    private Map<String, String> readColumnMap(File file) {
        Map<String, String> map = new HashMap<>();
        try (Stream<String> stream = Files.lines( Paths.get(file.getAbsolutePath()), StandardCharsets.UTF_8))
        {
            stream.forEach(s -> {
                String[] columns = s.split("=");
                map.put(columns[0],columns[1]);
            });
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return map;
    }

    public String getSqlDirectory() {
        return sqlDirectory;
    }

    public void setSqlDirectory(String sqlDirectory) {
        this.sqlDirectory = sqlDirectory;
    }

    public String getColumnMapDirectory() {
        return columnMapDirectory;
    }

    public void setColumnMapDirectory(String columnMapDirectory) {
        this.columnMapDirectory = columnMapDirectory;
    }

    public String getOutputDirectory() {
        return outputDirectory;
    }

    public void setOutputDirectory(String outputDirectory) {
        this.outputDirectory = outputDirectory;
    }
}
