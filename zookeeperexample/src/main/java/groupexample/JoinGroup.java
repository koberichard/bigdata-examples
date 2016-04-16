package groupexample;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

/**
 * the join group class
 * Created by matt on 4/15/16.
 */
public class JoinGroup extends ConnectionWatcher {
	public void join(String groupName, String memberName) throws KeeperException, InterruptedException {
		String path = "/" + groupName + "/" + memberName;
		String createdPath = zk.create(path, null/*data*/, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println("Created " + createdPath);
	}

	public static void main(String[] args) throws Exception {
		JoinGroup joinGroup = new JoinGroup();
		joinGroup.connect(args[0]);
		joinGroup.join(args[1], args[2]);
	}
}