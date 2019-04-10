package org.dxer.hbase.exceptions;

public class NoRowKeyException extends Exception {
    private static final long serialVersionUID = -8980028569652624236L;

    public NoRowKeyException(String string) {
        super(string);
    }

}
