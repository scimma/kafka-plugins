package scimma;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scimma.LockGuard;
import scimma.PeriociallySyncable;

///A background thread object to drive periodic full synchronizations with the database.
public class SyncThread extends Thread{
	protected static final Logger LOG = LoggerFactory.getLogger(ExternalScramAuthnCallbackHandler.class);
	
	///The object for which this thread triggers re-synchronization.
	private PeriociallySyncable syncObj;
	///A lock for managing orderly shutdown of the thread
	private Lock lock;
	///A condition variable based on lock, on which the thread will wait while sleeping.
	///lock must be held to manipulte this object.
	private Condition stopCondition;
	///A flag which indicates that the thread should cease running.
	///lock must be held to manipulate this object after construction. 
	private boolean stopFlag;
	
	SyncThread(PeriociallySyncable syncObj){
		this.syncObj = syncObj;
		this.lock = new ReentrantLock();
		this.stopCondition = lock.newCondition();
		this.stopFlag = false;
	}
	
	public void run(){
		LOG.debug("Sync thread started");
		try{
			//Kafka may end up starting a number of these threads, very close to the same time,
			//so include a random delay to try to keep database requests from being in phase.
			try(LockGuard g=new LockGuard(lock)){
				LOG.debug("Sync thread entering initial random sleep");
				if(stopCondition.await((int)(Math.random()*30), TimeUnit.SECONDS))
					return;
			}
			//main update loop
			while(true){
				syncObj.update();
				//check for instructions to terminate, and sleep before the next update.
				try(LockGuard g=new LockGuard(lock)){
					if(stopFlag)
						return;
					if(stopCondition.await(syncObj.getSyncPeriod(), TimeUnit.SECONDS))
						return;
				}
			}
		}
		catch(InterruptedException ex){
			LOG.debug("Sync thread interrupted");
		}
	}
	
	//can't name this stop because the base version is final, 
	//even though it's unsafe, deprecated, and should never be used
	public void end(){
		LOG.debug("Sync thread stopping");
		try(LockGuard g=new LockGuard(lock)){
			stopFlag = true;
			stopCondition.signal();
		}
	}
}
