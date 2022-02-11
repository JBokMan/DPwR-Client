package exceptions;

public class TooLongKeyException extends Exception {
    public static final long serialVersionUID = 4328743;
    public TooLongKeyException(final String errorMessage) {
        super(errorMessage);
    }
}
