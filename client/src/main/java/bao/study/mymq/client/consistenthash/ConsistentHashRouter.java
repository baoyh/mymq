package bao.study.mymq.client.consistenthash;

import bao.study.mymq.client.ClientException;

import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author baoyh
 * @since 2024/6/14 20:40
 */
public class ConsistentHashRouter {

    private final TreeMap<Long, VirtualNode<Node>> ring = new TreeMap<>();

    private final Map<String, Boolean> nodes = new ConcurrentHashMap<>();

    private final MessageDigest messageDigest;

    public ConsistentHashRouter() {
        try {
            this.messageDigest = MessageDigest.getInstance("MD5");
        } catch (Throwable e) {
            throw new ClientException(e.getMessage());
        }
    }

    public void addNode(Node node, int virtualCount) {
        if (node == null || node.getKey() == null || nodes.containsKey(node.getKey())) {
            return;
        }
        for (int i = 0; i < virtualCount; i++) {
            long hash = hash(node.getKey() + "-" + i);
            ring.put(hash, new VirtualNode<>(node));
        }
        nodes.put(node.getKey(), true);
    }

    public Node getNode(String key) {
        if (key == null) {
            return null;
        }
        long hash = hash(key);
        if (ring.isEmpty()) {
            return null;
        }
        Map.Entry<Long, VirtualNode<Node>> entry = ring.ceilingEntry(hash);
        if (entry == null) {
            entry = ring.firstEntry();
        }
        return entry.getValue().getNode();
    }

    public void removeNode(Node node) {
        if (node == null || node.getKey() == null || !nodes.containsKey(node.getKey())) {
            return;
        }
        Iterator<Long> it = ring.keySet().iterator();
        while (it.hasNext()) {
            Long key = it.next();
            VirtualNode<Node> virtualNode = ring.get(key);
            if (virtualNode.getNode().getKey().equals(node.getKey())) {
                it.remove();
            }
        }
        nodes.remove(node.getKey());
    }


    public long hash(String key) {
        messageDigest.reset();
        messageDigest.update(key.getBytes());
        byte[] digest = messageDigest.digest();

        long h = 0;
        for (int i = 0; i < 4; i++) {
            h <<= 8;
            h |= ((int) digest[i]) & 0xFF;
        }
        return h;
    }
}
