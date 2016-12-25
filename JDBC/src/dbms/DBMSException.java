package dbms;

public class DBMSException extends Exception{
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private String errorMessage;

	public DBMSException(String error) {
		this.errorMessage = error;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

}
