package scimma;

public interface PeriociallySyncable{
	public void update();
	///Get the length of time to wait between updates, in seconds.
	int getSyncPeriod();
	///Set the length of time to wait between updates, in seconds.
	///@param period the update period, in seconds. Must not be negative.
	void setSyncPeriod(int period);
}
