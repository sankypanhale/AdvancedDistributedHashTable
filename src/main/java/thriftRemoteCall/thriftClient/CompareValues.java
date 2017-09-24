package thriftRemoteCall.thriftClient;

public class CompareValues {

	int correct;
	int incorrect;
	public CompareValues()
	{
		correct = 0;
		incorrect = 0;
	}
	public int getCorrect() {
		return correct;
	}
	public void setCorrect(int correct) {
		this.correct = correct;
	}
	public int getIncorrect() {
		return incorrect;
	}
	public void setIncorrect(int incorrect) {
		this.incorrect = incorrect;
	}
}
