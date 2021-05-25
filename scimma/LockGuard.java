package scimma;

import java.util.concurrent.locks.Lock;

///An equivalent to std::lock_guard to allow using try-with-resources blocks to manage holding Locks.
public class LockGuard implements AutoCloseable{
	private Lock lock;
	public LockGuard(Lock l){
		this.lock=l;
		this.lock.lock();
	}
	public void close(){
		this.lock.unlock();
	}
}
