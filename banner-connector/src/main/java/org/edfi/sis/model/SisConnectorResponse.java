package org.edfi.sis.model;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;

public class SisConnectorResponse {
    private boolean fatalError = false;
    private String errorMessage;
    private long upsertCount;
    private long deleteCount;
    private DateTime startTime;
    private DateTime endTime;
    private long duration;
    private Exception exception;
    List<String> errors = new ArrayList<>();

    public boolean isFatalError() {
        return fatalError;
    }

    public void setFatalError(boolean fatalError) {
        this.fatalError = fatalError;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public long getUpsertCount() {
        return upsertCount;
    }

    public void setUpsertCount(long upsertCount) {
        this.upsertCount = upsertCount;
    }

    public long getDeleteCount() {
        return deleteCount;
    }

    public void setDeleteCount(long deleteCount) {
        this.deleteCount = deleteCount;
    }

    public DateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(DateTime startTime) {
        this.startTime = startTime;
    }

    public DateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(DateTime endTime) {
        this.endTime = endTime;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public List<String> getErrors() {
        return errors;
    }

    public void setErrors(List<String> errors) {
        this.errors = errors;
    }

    public void addError(String error) {
        this.getErrors().add(error);
    }

    public String buildReport() {
        final String LINE_DIVISION = String.format("-----------------------------------------%n");
        final String ERROR_LINE_DIVISION = String.format("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!%n");
        StringBuilder report = new StringBuilder();

        report.append(LINE_DIVISION)
                .append(String.format("%nTPDM SIS CONNECTOR REPORT:%n"));

        if (isFatalError()) {
            report.append(ERROR_LINE_DIVISION)
                    .append(ERROR_LINE_DIVISION)
                    .append(ERROR_LINE_DIVISION).append(String.format("ERROR:%n"))
                    .append(String.format("There was an error in the TPDM SIS CONNECTOR.%n"))
                    .append(String.format("Error Message:%n"))
                    .append(getErrorMessage())
                    .append(String.format("%n"))
                    .append(ERROR_LINE_DIVISION)
                    .append(ERROR_LINE_DIVISION)
                    .append(ERROR_LINE_DIVISION)
                    .append(String.format("%n"))
                    .append(String.format("EXCEPTION:%n"))
                    .append(ExceptionUtils.getStackTrace(getException()))
                    .append(String.format("%n"));
        }

        report.append(LINE_DIVISION)
                .append(String.format("Start Time: %s %n", getStartTime()))
                .append(String.format("  End Time: %s %n", getEndTime()))
                .append(String.format(" Exec Time: %s %n%n", getDuration()))
                .append(String.format("Upsert Count: %s %n", getUpsertCount()))
                .append(String.format("Delete Count: %s %n", getDeleteCount()))
                .append(LINE_DIVISION);

        if (errors.size() > 0) {
            report.append(String.format("%n"))
                    .append(LINE_DIVISION)
                    .append(String.format("Errors/Warnings:%n"))
                    .append(LINE_DIVISION);
            errors.forEach(error -> report.append(String.format("%s %n", error))
                                            .append(LINE_DIVISION));
        }
        return report.toString();
    }
}
