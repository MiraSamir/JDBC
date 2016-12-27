package fileHandler.protocolBuffer;

public class protocolBufferException extends Exception{

		private String errorMessage;

		public protocolBufferException(String error) {
			this.errorMessage = error;
		}

		public String getErrorMessage() {
			return errorMessage;
		}

		public void setErrorMessage(String errorMessage) {
			this.errorMessage = errorMessage;
		}




}
