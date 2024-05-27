package bao.study.mymq.broker.raft;

import bao.study.mymq.broker.raft.protocol.ClientProtocol;
import bao.study.mymq.common.protocol.raft.*;
import bao.study.mymq.broker.raft.store.RaftStore;
import bao.study.mymq.common.ServiceThread;
import bao.study.mymq.remoting.code.RequestCode;
import bao.study.mymq.remoting.code.ResponseCode;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 日志管理器, 功能等同于 DledgerEntryPusher
 *
 * @author baoyh
 * @since 2024/5/21 15:42
 */
public class EntryProcessor {

    private static final Logger logger = LoggerFactory.getLogger(EntryProcessor.class);

    private final Config config;

    private final MemberState memberState;

    private final RaftStore raftStore;

    private final ClientProtocol clientProtocol;

    private final EntryHandler entryHandler;

    private final QuorumAckChecker quorumAckChecker;

    private final Map<String, EntryDispatcher> entryDispatchers = new HashMap<>();

    /**
     * 每个节点基于投票轮次的当前水位线标记
     * 相当于是记录每个节点当前的 entry index
     */
    private Map<Long /* term */, ConcurrentMap<String /* node id */, Long /* entry index */>> peerWaterMarksByTerm = new ConcurrentHashMap<>();

    public EntryProcessor(MemberState memberState,ClientProtocol clientProtocol, RaftStore raftStore) {
        this.memberState = memberState;
        this.config = memberState.getConfig();
        this.clientProtocol = clientProtocol;
        this.raftStore = raftStore;
        this.entryHandler = new EntryHandler();
        this.quorumAckChecker = new QuorumAckChecker();
        memberState.getNodes().forEach((k,v) -> entryDispatchers.put(k, new EntryDispatcher()));
    }

    public void start() {
        entryHandler.start();
        quorumAckChecker.start();
        entryDispatchers.forEach((k, v) -> v.start());
    }

    public CompletableFuture<AppendEntryResponse> handleAppend(AppendEntryRequest entryRequest) {
        if (memberState.getRole() != Role.LEADER) {
            throw new RaftException("Only leader can append message");
        }

        RaftEntry entry = new RaftEntry();
        entry.setBody(entryRequest.getBody());
        RaftEntry response = raftStore.appendAsLeader(entry);
        return this.waitAck(response);
    }

    private CompletableFuture<AppendEntryResponse> waitAck(RaftEntry response) {

        return null;
    }

    private AppendEntryResponse createAppendEntryResponse(RaftEntry entry) {
        AppendEntryResponse entryResponse = new AppendEntryResponse();
        entryResponse.setPos(entry.getPos());
        entryResponse.setIndex(entry.getIndex());
        entryResponse.setTerm(memberState.getTerm());
        entryResponse.setCode(ResponseCode.SUCCESS);
        entryResponse.setLeaderId(memberState.getLeaderId());
        return entryResponse;
    }

    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest entryRequest) {
        try {
            return entryHandler.handlePush(entryRequest);
        } catch (Throwable e) {
            throw new RaftException(e.getMessage());
        }
    }

    private class EntryDispatcher extends ServiceThread {

        private AtomicReference<PushEntryRequest.Type> type = new AtomicReference<>(PushEntryRequest.Type.COMPARE);

        /**
         * 目标节点 ID
         */
        private String peerId;

        /**
         * 已完成比较的日志序号
         */
        private long compareIndex = -1;

        /**
         * 已写入的日志序号
         */
        private long writeIndex = -1;

        /**
         * 当前的投票轮次
         */
        private long term = -1;

        /**
         * 记录日志的等待时间
         */
        private ConcurrentMap<Long/* entry index */, Long /* 等待时间戳 */> pendingMap = new ConcurrentHashMap<>();

        @Override
        public String getServiceName() {
            return EntryDispatcher.class.getName();
        }

        @Override
        public void run() {
            while (!stop) {
                try {
                    if (memberState.getRole() != Role.LEADER) {
                        waitForRunning(1);
                        continue;
                    }
                    switch (type.get()) {
                        case APPEND:
                            doAppend();
                            break;
                        case COMPARE:
                            doCompare();
                            break;
                        case COMMIT:
                            doCommit();
                            break;
                    }
                    waitForRunning(1);
                } catch (Throwable ex) {
                    logger.info("", ex);
                }
            }
        }

        private void doCompare() throws Exception {
            if (compareIndex == -1 && raftStore.getEndIndex() == -1) {
                return;
            }
            compareIndex = raftStore.getEndIndex();
            RaftEntry raftEntry = raftStore.get(compareIndex);
            CompletableFuture<PushEntryResponse> future = clientProtocol.push(createPushEntryRequest(raftEntry, PushEntryRequest.Type.COMPARE, compareIndex));
            PushEntryResponse response = future.get(config.getRpcTimeoutMillis(), TimeUnit.MILLISECONDS);

            long truncateIndex = -1;
            if (response.getCode() == ResponseCode.SUCCESS) {
                if (compareIndex == response.getEndIndex()) {
                    changeState(compareIndex, PushEntryRequest.Type.APPEND);
                    return;
                } else {
                    truncateIndex = compareIndex;
                }
            } else if (response.getEndIndex() < raftStore.getBeginIndex() || response.getBeginIndex() > raftStore.getEndIndex()) {
                // 如果从节点存储的最大日志序号小于主节点的最小序号，或者从节点的最小日志序号大于主节点的最大日志序号，即两者不相交
                // 这通常发生在从节点崩溃很长一段时间. truncateIndex 设置为主节点的 ledgerBeginIndex，即主节点目前最小的偏移量
                truncateIndex = compareIndex;
                changeState(truncateIndex, PushEntryRequest.Type.TRUNCATE);
            } else if (compareIndex < response.getBeginIndex()) {
                // 如果已比较的日志序号小于从节点的开始日志序号，很可能是从节点磁盘发送损耗，从主节点最小日志序号开始同步
                truncateIndex = raftStore.getBeginIndex();
            } else if (compareIndex > response.getEndIndex()) {
                // 如果已比较的日志序号大于从节点的最大日志序号，则已比较索引设置为从节点最大的日志序号，触发数据的继续同步
                compareIndex = response.getEndIndex();
            } else {
                // 如果已比较的日志序号大于从节点的开始日志序号，但小于从节点的最大日志序号，则待比较索引减一
                compareIndex--;
            }
            // 如果比较出来的日志序号小于主节点的最小日志需要，则设置为主节点的最小序号
            if (compareIndex < raftStore.getBeginIndex()) {
                truncateIndex = raftStore.getBeginIndex();
            }
            // 如果比较出来的日志序号不等于 -1 ，则向从节点发送 TRUNCATE 请求
            if (truncateIndex != -1) {
                changeState(truncateIndex, PushEntryRequest.Type.TRUNCATE);
                doTruncate(truncateIndex);
            }
        }

        private void doTruncate(long truncateIndex) {

        }

        private void doCommit() {

        }

        private void doAppend() {

        }

        private void changeState(long index, PushEntryRequest.Type type) {
            switch (type) {
                case APPEND:
                    compareIndex = -1;
                    writeIndex = index + 1;
                    updatePeerWaterMark(term, peerId, index);
                    break;
                case COMPARE:
                    break;
                case TRUNCATE:
                    compareIndex = -1;
                    break;
            }
            this.type.set(type);
        }

        private PushEntryRequest createPushEntryRequest(RaftEntry raftEntry, PushEntryRequest.Type type, long commitIndex) {
            PushEntryRequest pushEntryRequest = new PushEntryRequest();
            pushEntryRequest.setEntry(raftEntry);
            pushEntryRequest.setType(type);
            pushEntryRequest.setCommitIndex(commitIndex);
            pushEntryRequest.setLocalId(memberState.getSelfId());
            pushEntryRequest.setRemoteId(peerId);
            pushEntryRequest.setLeaderId(memberState.getLeaderId());
            pushEntryRequest.setCode(RequestCode.APPEND);
            pushEntryRequest.setTerm(memberState.getTerm());
            return pushEntryRequest;
        }
    }

    private synchronized void updatePeerWaterMark(long term, String peerId, long index) {
        checkPeerWaterMark(term, peerId, index);
        if (peerWaterMarksByTerm.get(term).get(peerId) < index) {
            peerWaterMarksByTerm.get(term).put(peerId, index);
        }
    }

    private void checkPeerWaterMark(long term, String peerId, long index) {
        if (peerWaterMarksByTerm.containsKey(term)) {
            if (!peerWaterMarksByTerm.get(term).containsKey(peerId)) {
                ConcurrentMap<String, Long> map = new ConcurrentHashMap<>();
                map.put(peerId, index);
                peerWaterMarksByTerm.put(term, map);
            }
        } else {
            ConcurrentMap<String, Long> map = new ConcurrentHashMap<>();
            map.put(peerId, index);
            peerWaterMarksByTerm.put(term, map);
        }
    }

    private class EntryHandler extends ServiceThread {

        /**
         * append 请求处理队列
         */
        ConcurrentMap<Long, Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> writeRequestMap = new ConcurrentHashMap<>();

        /**
         * compare, truncate, commit 请求处理队列
         */
        BlockingQueue<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> compareOrTruncateRequests = new ArrayBlockingQueue<>(100);

        @Override
        public String getServiceName() {
            return EntryHandler.class.getName();
        }

        @Override
        public void run() {
            while (!stop) {
                if (memberState.getRole() != Role.FOLLOWER) {
                    waitForRunning(1);
                    continue;
                }

                if (compareOrTruncateRequests.peek() != null) {
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = compareOrTruncateRequests.poll();
                    PushEntryRequest request = pair.getKey();
                    switch (request.getType()) {
                        case COMPARE:
                            this.handleCompare(request.getEntry().getIndex(), request, pair.getValue());
                        case TRUNCATE:
                            this.handleTruncate(request.getEntry().getIndex(), request, pair.getValue());
                        case COMMIT:
                            this.handleCommit(request.getCommitIndex(), request, pair.getValue());
                    }
                } else {
                    long nextIndex = raftStore.getEndIndex() + 1;
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.remove(nextIndex);
                    if (pair != null) {
                        this.handleAppend(nextIndex, pair.getKey(), pair.getValue());
                    }
                }
            }
        }

        /**
         * 接收 leader 发送的的 push 请求, 并将其放入处理队列中
         */
        public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest entryRequest) throws InterruptedException {
            PushEntryRequest.Type type = entryRequest.getType();
            CompletableFuture<PushEntryResponse> future = new CompletableFuture<>();
            if (type == PushEntryRequest.Type.APPEND) {
                writeRequestMap.put(entryRequest.getEntry().getIndex(), new Pair<>(entryRequest, future));
            } else if (type == PushEntryRequest.Type.COMMIT) {
                compareOrTruncateRequests.put(new Pair<>(entryRequest, future));
            } else {
                writeRequestMap.clear();
                compareOrTruncateRequests.put(new Pair<>(entryRequest, future));
            }
            return future;
        }

        private void handleCommit(long commitIndex, PushEntryRequest request, CompletableFuture<PushEntryResponse> value) {

        }

        private void handleTruncate(long truncateIndex, PushEntryRequest request, CompletableFuture<PushEntryResponse> value) {

        }

        private void handleCompare(long compareIndex, PushEntryRequest request, CompletableFuture<PushEntryResponse> future) {
            RaftEntry entry = raftStore.get(compareIndex);
            if (request.getEntry().equals(entry)) {
                future.complete(createPushEntryResponse(request, ResponseCode.SUCCESS));
            } else {
                future.complete(createPushEntryResponse(request, ResponseCode.INCONSISTENT_STATE));
            }
        }

        private void handleAppend(long writeIndex, PushEntryRequest request, CompletableFuture<PushEntryResponse> future) {

        }

        private PushEntryResponse createPushEntryResponse(PushEntryRequest request, int code) {
            PushEntryResponse response = new PushEntryResponse();
            response.setCode(code);
            response.setBeginIndex(raftStore.getBeginIndex());
            response.setEndIndex(raftStore.getEndIndex());
            response.setIndex(request.getEntry().getIndex());
            return response;
        }
    }

    private class QuorumAckChecker extends ServiceThread {

        @Override
        public String getServiceName() {
            return QuorumAckChecker.class.getName();
        }

        @Override
        public void run() {
            while (!stop) {
                if (memberState.getRole() != Role.LEADER) {
                    waitForRunning(1);
                    continue;
                }
            }
        }

    }
}