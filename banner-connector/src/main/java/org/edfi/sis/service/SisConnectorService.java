package org.edfi.sis.service;

import com.google.gson.annotations.SerializedName;
import com.opencsv.RFC4180Parser;
import org.apache.commons.lang3.StringUtils;
import org.edfi.api.ApiClient;
import org.edfi.api.ApiException;
import org.edfi.api.descriptor.*;
import org.edfi.api.resource.TeacherCandidatesApi;
import org.edfi.model.descriptor.*;
import org.edfi.model.resource.*;
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

    @Value( "${tpdm.api.save}" )
    boolean saveToTPDM;

    @Value( "${output.data.to.dir}" )
    boolean outputDataToDir;

    String nowDate = null;
    String nowTime = null;

    public static final String TEACHER_CANDIDATE_IDS_SQL_NAME = "teacherCandidateIds";
    public static final String TEACHER_CANDIDATE_SQL_NAME = "teacherCandidate";
    public static final String TEACHER_CANDIDATE_ADDRESSES_SQL_NAME = "teacherCandidateAddresses";

    Map<String, EdFiAddressTypeDescriptor> addressTypeDescriptorMap = null;
    Map<String, EdFiLocaleDescriptor> localeDescriptorMap = null;
    Map<String, EdFiStateAbbreviationDescriptor> stateAbbreviationDescriptorMap = null;
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

            DateTimeFormatter formatDate = DateTimeFormat.forPattern("YYYYMMdd");
            DateTimeFormatter formatTime = DateTimeFormat.forPattern("HHmmss");
            DateTime dateTime = DateTime.now();
            nowDate = dateTime.toString(formatDate);
            nowTime = dateTime.toString(formatTime);

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
        columnsMap = loadColumnsMap();
        addressTypeDescriptorMap = loadAddressTypeDescriptorsMap();
        localeDescriptorMap = loadLocaleDescriptorsMap();
        stateAbbreviationDescriptorMap = loadStateAbbreviationDescriptorsMap();
        academicSubjectDescriptorMap = loadAcademicSubjectDescriptorsMap();
        gradeLevelDescriptorMap = loadGradeLevelDescriptorsMap();
        tppDegreeTypeDescriptorMap = loadTppDegreeTypeDescriptorsMap();
        sexDescriptorMap = loadSexDescriptorsMap();
        existingTeacherCandidateMap = loadExistingTeacherCandidateMap();
    }

    private void processTeacherCandidate(SisConnectorResponse response) throws AuthenticationException, ApiException {
        int upsertCount = 0;

        List<String> studentUniqueIds = retrieveStudentUniqueIds();

        for (String studentUniqueId : studentUniqueIds) {
            TpdmTeacherCandidate teacherCandidate = retrieveTeacherCandidate(studentUniqueId);
            List<TpdmTeacherCandidateAddress> teacherCandidateAddresses = retrieveTeacherCandidateAddresses(studentUniqueId);
            teacherCandidate.setAddresses(teacherCandidateAddresses);
            try {
                saveTeacherCandidate(teacherCandidate);
                existingTeacherCandidateMap.remove(teacherCandidate.getTeacherCandidateIdentifier());
                upsertCount++;
            } catch (ApiException ae) {
                response.addError(teacherCandidate.toString() + String.format("%n") + ae.getResponseBody());
            }
        }
        response.setUpsertCount(upsertCount);
    }

    private void saveTeacherCandidate(TpdmTeacherCandidate teacherCandidate) throws AuthenticationException, ApiException {
        if (saveToTPDM) {
            try {
                TeacherCandidatesApi teacherCandidatesApi = new TeacherCandidatesApi(apiClient);
                teacherCandidatesApi.postTeacherCandidate(teacherCandidate);
                existingTeacherCandidateMap.remove(teacherCandidate.getTeacherCandidateIdentifier());
            } catch (ApiException ae) {
                if (ae.getCode() == (HttpStatus.UNAUTHORIZED.value())) {
                    apiClient.setAccessToken(tokenRetriever.obtainNewBearerToken());
                    TeacherCandidatesApi teacherCandidatesApi = new TeacherCandidatesApi(apiClient);
                    teacherCandidatesApi.postTeacherCandidate(teacherCandidate);
                } else {
                    throw ae;
                }
            }
        }
        if (outputDataToDir) {
            BufferedWriter writer = null;
            try {
                String fileName = getOutputDirectory() + "/" + nowDate + "/" +  nowTime + "/" +
                        teacherCandidate.getStudentReference().getStudentUniqueId() + ".out";
                File file = new File(fileName);
                file.getParentFile().mkdirs();
                writer = new BufferedWriter(new FileWriter(file));
                writer.write(teacherCandidate.toString());
            }
            catch ( IOException e) {
                String error = "";
            } finally {
                try {
                    if ( writer != null)
                        writer.close( );
                } catch ( IOException e) {
                }
            }
        }
    }

    private List<String> retrieveStudentUniqueIds() {
        boolean firstRow = true;
        int studentUniqueIdIndex = 0;
        List<String> studentUniqueIds = new ArrayList<>();

        List<List<String>> result = dao.makeSqlCall(sqlMap.get(TEACHER_CANDIDATE_IDS_SQL_NAME));
        for (List<String> row : result) {
            if (firstRow) {
                int currentIndex = 0;
                for (String header : row) {
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_IDS_SQL_NAME).get("studentUniqueId"))) {
                        studentUniqueIdIndex = currentIndex;
                    }
                    currentIndex++;
                }
                firstRow = false;
            } else {
                studentUniqueIds.add(row.get(studentUniqueIdIndex));
            }
        }
        return studentUniqueIds;
    }

    private TpdmTeacherCandidate retrieveTeacherCandidate(String studentUniqueId) {
        boolean firstRow = true;
        int teacherCandidateIdIndex = -1;
        int firstNameIndex = -1;
        int middleNameIndex = -1;
        int lastNameIndex = -1;
        int birthDateIndex = -1;
        int academicSubjectDescriptorIndex = -1;
        int gradeLevelDescriptorIndex = -1;
        int tppDegreeTypeDescriptorIndex = -1;
        int studentUniqueIdIndex = -1;
        int sexIndex = -1;

        TpdmTeacherCandidate teacherCandidate = null;

        List<List<String>> result = dao.makeSqlCall(sqlMap.get(TEACHER_CANDIDATE_SQL_NAME), studentUniqueId);
        for (List<String> row : result) {
            if (firstRow) {
                int currentIndex = 0;
                for (String header : row) {
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_SQL_NAME).get("teacherCandidateId"))) {
                        teacherCandidateIdIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_SQL_NAME).get("firstName"))) {
                        firstNameIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_SQL_NAME).get("middleName"))) {
                        middleNameIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_SQL_NAME).get("lastName"))) {
                        lastNameIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_SQL_NAME).get("birthDate"))) {
                        birthDateIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_SQL_NAME).get("academicSubjectDescriptor"))) {
                        academicSubjectDescriptorIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_SQL_NAME).get("gradeLevelDescriptor"))) {
                        gradeLevelDescriptorIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_SQL_NAME).get("tppDegreeTypeDescriptor"))) {
                        tppDegreeTypeDescriptorIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_SQL_NAME).get("studentUniqueId"))) {
                        studentUniqueIdIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get("teacherCandidate").get("sex"))) {
                        sexIndex = currentIndex;
                    }
                    currentIndex++;
                }
                firstRow = false;
            } else {
                String teacherCandidateId = getString(teacherCandidateIdIndex, row);
                String firstName = getString(firstNameIndex, row);
                String middleName = getString(middleNameIndex, row);
                String lastName = getString(lastNameIndex, row);
                String birthDate = getString(birthDateIndex, row);
                String academicSubjectDescriptor = getString(academicSubjectDescriptorIndex, row);
                String gradeLevelDescriptor = getString(gradeLevelDescriptorIndex, row);
                String tppDegreeTypeDescriptor = getString(tppDegreeTypeDescriptorIndex, row);
                String studentId = getString(studentUniqueIdIndex, row);
                String sex = getString(sexIndex, row);

                teacherCandidate = createTeacherCandidate(teacherCandidateId, firstName, middleName, lastName,
                        birthDate, academicSubjectDescriptor, gradeLevelDescriptor, tppDegreeTypeDescriptor,
                        studentId, sex);
            }
        }
        return teacherCandidate;
    }

    private List<TpdmTeacherCandidateAddress> retrieveTeacherCandidateAddresses (String studentUniqueId) {
        boolean firstRow = true;
        int addressTypeDescriptorIndex = -1;
        int localDescriptorIndex = -1;
        int stateAbbreviationDescriptorIndex = -1;
        int apartmentRoomSuiteNumberIndex = -1;
        int buildingSiteNumberIndex = -1;
        int cityIndex = -1;
        int congressionalDistrictIndex = -1;
        int countyFIPSCodeIndex = -1;
        int doNotPublishIndicatorIndex = -1;
        int nameOfCountyIndex = -1;
        int postalCodeIndex = -1;
        int streetNumberNameIndex = -1;
        int periodBeginDateIndex = -1;
        int periodEndDateIndex = -1;

        List<TpdmTeacherCandidateAddress> teacherCandidateAddresses = new ArrayList<>();

        List<List<String>> result = dao.makeSqlCall(sqlMap.get(TEACHER_CANDIDATE_ADDRESSES_SQL_NAME),studentUniqueId);
        for (List<String> row : result) {
            if (firstRow) {
                int currentIndex = 0;
                for (String header : row) {
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_ADDRESSES_SQL_NAME).get("addressTypeDescriptor"))) {
                        addressTypeDescriptorIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_ADDRESSES_SQL_NAME).get("localDescriptor"))) {
                        localDescriptorIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_ADDRESSES_SQL_NAME).get("stateAbbreviationDescriptor"))) {
                        stateAbbreviationDescriptorIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_ADDRESSES_SQL_NAME).get("apartmentRoomSuiteNumber"))) {
                        apartmentRoomSuiteNumberIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_ADDRESSES_SQL_NAME).get("buildingSiteNumber"))) {
                        buildingSiteNumberIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_ADDRESSES_SQL_NAME).get("city"))) {
                        cityIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_ADDRESSES_SQL_NAME).get("congressionalDistrict"))) {
                        congressionalDistrictIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_ADDRESSES_SQL_NAME).get("countyFIPSCode"))) {
                        countyFIPSCodeIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_ADDRESSES_SQL_NAME).get("doNotPublishIndicator"))) {
                        doNotPublishIndicatorIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_ADDRESSES_SQL_NAME).get("nameOfCounty"))) {
                        nameOfCountyIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_ADDRESSES_SQL_NAME).get("postalCode"))) {
                        postalCodeIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_ADDRESSES_SQL_NAME).get("streetNumberName"))) {
                        streetNumberNameIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_ADDRESSES_SQL_NAME).get("periodBeginDate"))) {
                        periodBeginDateIndex = currentIndex;
                    }
                    if (StringUtils.equalsAnyIgnoreCase(header, columnsMap.get(TEACHER_CANDIDATE_ADDRESSES_SQL_NAME).get("periodEndDate"))) {
                        periodEndDateIndex = currentIndex;
                    }
                    currentIndex++;
                }
                firstRow = false;
            } else {
                String addressTypeDescriptor = getString(addressTypeDescriptorIndex, row);
                String localDescriptor = getString(localDescriptorIndex, row);
                String stateAbbreviationDescriptor = getString(stateAbbreviationDescriptorIndex, row);
                String apartmentRoomSuiteNumber = getString(apartmentRoomSuiteNumberIndex, row);
                String buildingSiteNumber = getString(buildingSiteNumberIndex, row);
                String city = getString(cityIndex, row);
                String congressionalDistrict = getString(congressionalDistrictIndex, row);
                String countyFIPSCode = getString(countyFIPSCodeIndex, row);
                String doNotPublishIndicator = getString(doNotPublishIndicatorIndex, row);
                String nameOfCounty = getString(nameOfCountyIndex, row);
                String postalCode = getString(postalCodeIndex, row);
                String streetNumberName = getString(streetNumberNameIndex, row);
                String periodBeginDate = getString(periodBeginDateIndex, row);
                String periodEndDate = getString(periodEndDateIndex, row);

                TpdmTeacherCandidateAddress teacherCandidateAddress = createTeacherCandidateAddress(
                        addressTypeDescriptor, localDescriptor, stateAbbreviationDescriptor, apartmentRoomSuiteNumber,
                        buildingSiteNumber, city, congressionalDistrict, countyFIPSCode, doNotPublishIndicator,
                        nameOfCounty, postalCode, streetNumberName, periodBeginDate, periodEndDate);
                addTeacherAddress(teacherCandidateAddresses, teacherCandidateAddress);
            }
        }
        return teacherCandidateAddresses;
    }

    private void addTeacherAddress(List<TpdmTeacherCandidateAddress> list, TpdmTeacherCandidateAddress newAddress) {
        for (TpdmTeacherCandidateAddress address: list) {
            if (address.getStreetNumberName().equals(newAddress.getStreetNumberName()) &&
                    address.getCity().equals(newAddress.getCity()) &&
                    address.getStateAbbreviationDescriptor().equals(newAddress.getStateAbbreviationDescriptor()) &&
                    address.getPostalCode().equals(newAddress.getPostalCode()) ) {
                for (TpdmTeacherCandidateAddressPeriod period : address.getPeriods()) {
                    if (newAddress.getPeriods().get(0).equals(period)){
                        return;
                    }
                }
                address.getPeriods().add(newAddress.getPeriods().get(0));
                return;
            }
        }
        list.add(newAddress);
    }

    private TpdmTeacherCandidate createTeacherCandidate(String teacherCandidateId, String firstName, String middleName,
                                                        String lastName, String birthDate, String academicSubject,
                                                        String gradeLevel, String degreeType, String studentId,
                                                        String sex) {
        TpdmTeacherCandidate teacherCandidate = new TpdmTeacherCandidate();
        teacherCandidate.setTeacherCandidateIdentifier(teacherCandidateId);
        teacherCandidate.setFirstName(firstName);
        teacherCandidate.setLastSurname(lastName);
        teacherCandidate.setMiddleName(middleName);
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

    private TpdmTeacherCandidateAddress createTeacherCandidateAddress(String addressTypeDescriptor, String localDescriptor,
                String stateAbbreviationDescriptor, String apartmentRoomSuiteNumber, String buildingSiteNumber, String city,
                String congressionalDistrict, String countyFIPSCode, String doNotPublishIndicator, String nameOfCounty,
                String postalCode, String streetNumberName, String periodBeginDate, String periodEndDate) {

        TpdmTeacherCandidateAddress address = new TpdmTeacherCandidateAddress();
        address.setAddressTypeDescriptor(getAddressTypeDescriptorUri(addressTypeDescriptor));
        address.setLocaleDescriptor(getLocaleDescriptorUri(localDescriptor));
        address.stateAbbreviationDescriptor(getStateAbbreviationDescriptorUri(stateAbbreviationDescriptor));
        address.setApartmentRoomSuiteNumber(apartmentRoomSuiteNumber);
        address.setBuildingSiteNumber(buildingSiteNumber);
        address.setCity(city);
        address.setCongressionalDistrict(congressionalDistrict);
        address.setCountyFIPSCode(countyFIPSCode);
        address.setDoNotPublishIndicator(Boolean.valueOf(doNotPublishIndicator));
        address.setNameOfCounty(nameOfCounty);
        address.setPostalCode(postalCode);
        address.setStreetNumberName(streetNumberName);
        TpdmTeacherCandidateAddressPeriod period = new TpdmTeacherCandidateAddressPeriod();
        period.setBeginDate(LocalDate.parse(periodBeginDate));
        period.setEndDate(LocalDate.parse(periodEndDate));
        List<TpdmTeacherCandidateAddressPeriod> periods = new ArrayList<>();
        periods.add(period);
        address.periods(periods);
        return address;
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
        if (saveToTPDM) {
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
    }

    private Map<String, EdFiAddressTypeDescriptor> loadAddressTypeDescriptorsMap() throws ApiException, AuthenticationException {
        List<EdFiAddressTypeDescriptor> list = null;
        try {
            AddressTypeDescriptorsApi addressTypeDescriptorsApi = new AddressTypeDescriptorsApi(apiClient);
            list = addressTypeDescriptorsApi.getAddressTypeDescriptors(0, 100, false, null);
        } catch (ApiException ae) {
            if (ae.getCode() == (HttpStatus.UNAUTHORIZED.value())) {
                apiClient.setAccessToken(tokenRetriever.obtainNewBearerToken());
                AddressTypeDescriptorsApi addressTypeDescriptorsApi = new AddressTypeDescriptorsApi(apiClient);
                list = addressTypeDescriptorsApi.getAddressTypeDescriptors(0, 100, false, null);
            } else {
                throw ae;
            }
        }
        Map<String, EdFiAddressTypeDescriptor> map = new HashMap<>();
        for (EdFiAddressTypeDescriptor addressTypeDescriptor : list) {
            map.put(addressTypeDescriptor.getCodeValue(), addressTypeDescriptor);
        }
        return map;
    }

    private String getAddressTypeDescriptorUri (String code) {
        EdFiAddressTypeDescriptor addressTypeDescriptor = addressTypeDescriptorMap.get(code);
        if (addressTypeDescriptor!=null) {
            return addressTypeDescriptor.getNamespace() + "#" + code;
        }
        return code;
    }

    private Map<String, EdFiLocaleDescriptor> loadLocaleDescriptorsMap() throws ApiException, AuthenticationException {
        List<EdFiLocaleDescriptor> list = null;
        try {
            LocaleDescriptorsApi localeDescriptorsApi = new LocaleDescriptorsApi(apiClient);
            list = localeDescriptorsApi.getLocaleDescriptors(0, 100, false, null);
        } catch (ApiException ae) {
            if (ae.getCode() == (HttpStatus.UNAUTHORIZED.value())) {
                apiClient.setAccessToken(tokenRetriever.obtainNewBearerToken());
                LocaleDescriptorsApi localeDescriptorsApi = new LocaleDescriptorsApi(apiClient);
                list = localeDescriptorsApi.getLocaleDescriptors(0, 100, false, null);
            } else {
                throw ae;
            }
        }
        Map<String, EdFiLocaleDescriptor> map = new HashMap<>();
        for (EdFiLocaleDescriptor localeDescriptor : list) {
            map.put(localeDescriptor.getCodeValue(), localeDescriptor);
        }
        return map;
    }

    private String getLocaleDescriptorUri (String code) {
        EdFiLocaleDescriptor localeDescriptor = localeDescriptorMap.get(code);
        if (localeDescriptor!=null) {
            return localeDescriptor.getNamespace() + "#" + code;
        }
        return code;
    }

    private Map<String, EdFiStateAbbreviationDescriptor> loadStateAbbreviationDescriptorsMap() throws ApiException, AuthenticationException {
        List<EdFiStateAbbreviationDescriptor> list = null;
        try {
            StateAbbreviationDescriptorsApi stateAbbreviationDescriptorsApi = new StateAbbreviationDescriptorsApi(apiClient);
            list = stateAbbreviationDescriptorsApi.getStateAbbreviationDescriptors(0, 100, false, null);
        } catch (ApiException ae) {
            if (ae.getCode() == (HttpStatus.UNAUTHORIZED.value())) {
                apiClient.setAccessToken(tokenRetriever.obtainNewBearerToken());
                StateAbbreviationDescriptorsApi stateAbbreviationDescriptorsApi = new StateAbbreviationDescriptorsApi(apiClient);
                list = stateAbbreviationDescriptorsApi.getStateAbbreviationDescriptors(0, 100, false, null);
            } else {
                throw ae;
            }
        }
        Map<String, EdFiStateAbbreviationDescriptor> map = new HashMap<>();
        for (EdFiStateAbbreviationDescriptor stateAbbreviationDescriptor : list) {
            map.put(stateAbbreviationDescriptor.getCodeValue(), stateAbbreviationDescriptor);
        }
        return map;
    }

    private String getStateAbbreviationDescriptorUri (String code) {
        EdFiStateAbbreviationDescriptor stateAbbreviationDescriptor = stateAbbreviationDescriptorMap.get(code);
        if (stateAbbreviationDescriptor!=null) {
            return stateAbbreviationDescriptor.getNamespace() + "#" + code;
        }
        return code;
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
        return getOutputDirectory() + "/" +  nowDate + "-" + nowTime + ".report";
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

    private String getString(int index, List<String> row) {
        if (index >= 0) {
            return row.get(index);
        } else {
            return null;
        }
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
