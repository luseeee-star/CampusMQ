package com.campusslot.common.mq;

public class MqSendException extends RuntimeException {

    public MqSendException(String message, Throwable cause) {
        super(message, cause);
    }

    public MqSendException(String message) {
        super(message);
    }
}
