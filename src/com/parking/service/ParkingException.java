package com.parking.service;

public class ParkingException extends Exception {

    private final String errorCode;

    public ParkingException(String errorCode,
                            String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public ParkingException(String errorCode,
                            String message,
                            Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public String getErrorCode() {
        return errorCode;
    }

    @Override
    public String toString() {
        return "ParkingException{" +
               "code="    + errorCode   +
               ", message=" + getMessage() +
               "}";
    }
}
