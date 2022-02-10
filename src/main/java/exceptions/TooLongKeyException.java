package exceptions;

public class TooLongKeyException extends Exception {
    public TooLongKeyException(String errorMessage) {
        super(errorMessage);
    }
}
