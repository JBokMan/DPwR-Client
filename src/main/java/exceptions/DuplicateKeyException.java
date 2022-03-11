package exceptions;

public class DuplicateKeyException extends Exception {
    public static final long serialVersionUID = 4328789;

    public DuplicateKeyException(final String errorMessage) {
        super(errorMessage);
    }
}
