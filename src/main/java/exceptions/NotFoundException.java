package exceptions;

public class NotFoundException extends Exception {
    public static final long serialVersionUID = 4328742;

    public NotFoundException(final String errorMessage) {
        super(errorMessage);
    }
}
