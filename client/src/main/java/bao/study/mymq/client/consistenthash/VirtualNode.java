package bao.study.mymq.client.consistenthash;

/**
 * @author baoyh
 * @since 2024/6/14 20:40
 */
public class VirtualNode<T extends Node> {

    /**
     * physical node
     */
    private T node;

    public VirtualNode(T node) {
        this.node = node;
    }

    public T getNode() {
        return node;
    }

    public void setNode(T node) {
        this.node = node;
    }
}
