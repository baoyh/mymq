package bao.study.mymq.client.consistenthash;

/**
 * @author baoyh
 * @since 2024/6/14 20:44
 */
public class AllocateMessageQueueConsistentHash {

    private final ConsistentHashRouter consistentHashRouter = new ConsistentHashRouter();

    private final int virtualCount;

    public AllocateMessageQueueConsistentHash(int virtualCount) {
        this.virtualCount = virtualCount;
    }

    public String get(String key) {
        return consistentHashRouter.getNode(key).getKey();
    }

    public void add(String key) {
        consistentHashRouter.addNode(new NodeImpl(key), virtualCount);
    }

    public void remove(String key) {
        consistentHashRouter.removeNode(new NodeImpl(key));
    }


    static class NodeImpl implements Node {

        /**
         * broker id or message queue hash
         */
        private final String key;

        public NodeImpl(String key) {
            this.key = key;
        }

        @Override
        public String getKey() {
            return key;
        }
    }
}
