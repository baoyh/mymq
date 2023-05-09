package bao.study.mymq.broker.store;

/**
 * ConsumeQueue 用于提高消费的速度, 作用是充当消息在 CommitLog 中的索引
 * 消费时根据 Topic/Queue/Index 找到对应的索引文件
 * 文件中每条数据都被设计成定长 commitlog offset(8) + message size(4)
 *
 * @author baoyh
 * @since 2023/5/9 14:06
 */
public class ConsumeQueue {

    private MappedFile mappedFile;

    private String topic;

    private Integer queue;
}
