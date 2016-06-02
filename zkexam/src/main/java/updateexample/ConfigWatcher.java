package updateexample;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;

/**
 * Created by matt on 4/16/16.
 */
public class ConfigWatcher implements Watcher {
	private ActiveKeyValueStore store;

	public ConfigWatcher(String hosts) throws IOException, InterruptedException {
		store = new ActiveKeyValueStore();
		store.connect(hosts);
	}

	public void displayConfig() throws InterruptedException, KeeperException {
		String value = store.read(ConfigUpdater.PATH, this);
		System.out.printf("Read %s as %s.\n", ConfigUpdater.PATH, value);
	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getType() == Event.EventType.NodeChildrenChanged) {
			try {
				displayConfig();
			} catch (InterruptedException e) {
				System.out.println("Interrupted. exiting.");
				Thread.currentThread().interrupt();
			} catch (KeeperException e) {
				System.out.printf("KeeperException: %s. Exiting.\n", e);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		ConfigWatcher configWatcher = new ConfigWatcher(args[0]);
		configWatcher.displayConfig();

		Thread.sleep(Long.MAX_VALUE);
	}
}
