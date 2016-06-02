package groupexample;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by matt on 4/15/16.
 */
public class ConnectionWatcher implements Watcher{
	private static final int SESSION_TIMEOUT=5000;
	protected ZooKeeper zk;
	private CountDownLatch connectedSignal=new CountDownLatch(1);

	public void connect(String hosts) throws IOException, InterruptedException{
		zk=new ZooKeeper(hosts,SESSION_TIMEOUT,this);
		connectedSignal.await();
	}

	@Override
	public void process(WatchedEvent event){
		if(event.getState()== Event.KeeperState.SyncConnected){
			connectedSignal.countDown();
		}
	}

	public void close() throws  InterruptedException{
		zk.close();
	}
}
