package dbms;

public class DatabaseException extends Exception {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private String errorMessage;

	public DatabaseException(String error) {
		this.errorMessage = error;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

}
 