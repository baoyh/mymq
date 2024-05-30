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

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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
    private final Map<Long /* term */, ConcurrentMap<String /* node id */, Long /* entry index */>> peerWaterMarksByTerm = new ConcurrentHashMap<>();

    /**
     * 用于存放客户端发起的追加请求的响应结果
     */
    private final Map<Long /* term */, ConcurrentMap<Long /* entry index */, CompletableFuture<AppendEntryResponse>>> pendingAppendResponsesByTerm = new ConcurrentHashMap<>();

    /**
     * 暂时存放服务端对客户端请求的结果
     */
    private final Map<Long /* term */, ConcurrentMap<Long /* entry index */, AppendEntryResponse>> pendingServerResponsesByTerm = new ConcurrentHashMap<>();


    public EntryProcessor(MemberState memberState, ClientProtocol clientProtocol, RaftStore raftStore) {
        this.memberState = memberState;
        this.config = memberState.getConfig();
        this.clientProtocol = clientProtocol;
        this.raftStore = raftStore;
        this.entryHandler = new EntryHandler();
        this.quorumAckChecker = new QuorumAckChecker();
    }

    public void start() {
        entryHandler.start();
        quorumAckChecker.start();
        if (entryDispatchers.isEmpty()) {
            memberState.getNodes().forEach((k, v) -> {
                if (!k.equals(memberState.getSelfId())) {
                    EntryDispatcher entryDispatcher = new EntryDispatcher(k);
                    entryDispatchers.put(k, entryDispatcher);
                    entryDispatcher.start();
                }
            });
        } else {
            entryDispatchers.forEach((k, v) -> v.start());
        }
    }

    public CompletableFuture<AppendEntryResponse> handleAppend(AppendEntryRequest entryRequest) {
        if (memberState.getRole() != Role.LEADER) {
            throw new RaftException("Only leader can append message");
        }

        RaftEntry entry = new RaftEntry();
        entry.setBody(entryRequest.getBody());
        entry.setTerm(memberState.getTerm());
        RaftEntry response = raftStore.append(entry);
        return this.waitAck(response);
    }

    /**
     * 向 follower 发送日志, 并等待响应
     */
    private CompletableFuture<AppendEntryResponse> waitAck(RaftEntry entry) {
        // 更新自身节点的 index
        updatePeerWaterMark(entry.getTerm(), memberState.getSelfId(), entry.getIndex());
        if (memberState.getNodes().size() == 1) {
            return CompletableFuture.completedFuture(createAppendEntryResponse(entry));
        } else {
            checkPendingAppend(entry.getTerm());
            CompletableFuture<AppendEntryResponse> future = new CompletableFuture<>();
            pendingAppendResponsesByTerm.get(entry.getTerm()).put(entry.getIndex(), future);
            pendingServerResponsesByTerm.get(entry.getTerm()).put(entry.getIndex(), createAppendEntryResponse(entry));
            return future;
        }
    }

    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest entryRequest) {
        try {
            return entryHandler.handlePush(entryRequest);
        } catch (Throwable e) {
            throw new RaftException(e.getMessage());
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
            memberState.getNodes().forEach((k, v) -> map.put(k, -1L));
            map.put(peerId, index);
            peerWaterMarksByTerm.put(term, map);
        }
    }


    private void checkPendingAppend(long term) {
        if (!pendingAppendResponsesByTerm.containsKey(term)) {
            ConcurrentMap<Long, CompletableFuture<AppendEntryResponse>> map = new ConcurrentHashMap<>();
            pendingAppendResponsesByTerm.put(term, map);
        }
        if (!pendingServerResponsesByTerm.containsKey(term)) {
            pendingServerResponsesByTerm.put(term, new ConcurrentHashMap<>());
        }
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


    /**
     * 只适用于 leader, 负责处理日志的转发, 同步
     * 和 dledger 的设计有些出入, 当 follower 的 endIndex 小于 leader 的 beginIndex 时, 状态将变为 APPEND, 发起数据的同步
     */
    private class EntryDispatcher extends ServiceThread {

        private AtomicReference<PushEntryRequest.Type> type = new AtomicReference<>(PushEntryRequest.Type.COMPARE);

        /**
         * 目标节点 ID
         */
        private final String peerId;

        /**
         * 下一个需要 compare 的日志序号
         */
        private long compareIndex = -1;

        /**
         * 下一个需要 append 的日志序号
         */
        private long appendIndex = -1;

        /**
         * follower 最新的日志序号
         */
        private long followerEndIndex = -1;

        /**
         * 上一次提交的日志序号
         */
        private long lastCommittedIndex = -1;

        public EntryDispatcher(String peerId) {
            this.peerId = peerId;
        }

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
                        case TRUNCATE:
                            doTruncate();
                            break;
                    }
                    waitForRunning(1);
                } catch (Throwable ex) {
                    logger.info("", ex);
                }
            }
        }

        private void doCompare() throws Exception {
            if (raftStore.getEndIndex() == -1) {
                return;
            }
            if (followerEndIndex == raftStore.getEndIndex()) {
                return;
            }
            long endIndex = raftStore.getEndIndex();
            if (compareIndex == -1) {
                compareIndex = endIndex;
            }
            RaftEntry raftEntry = raftStore.get(compareIndex);
            CompletableFuture<PushEntryResponse> future = clientProtocol.push(createPushEntryRequest(raftEntry, PushEntryRequest.Type.COMPARE, compareIndex));
            PushEntryResponse response = future.get(config.getRpcTimeoutMillis(), TimeUnit.MILLISECONDS);

            if (response.getCode() == ResponseCode.INCONSISTENT_STATE) {
                appendIndex = compareIndex;
                changeState(PushEntryRequest.Type.APPEND);
                return;
            }
            followerEndIndex = response.getEndIndex();

            if (endIndex < response.getBeginIndex()) {
                // 如果主节点最大日志序号小于从节点的开始日志序号，很可能是从节点磁盘发送损耗，从主节点最小日志序号开始同步
                appendIndex = raftStore.getBeginIndex();
                changeState(PushEntryRequest.Type.APPEND);
            } else if (endIndex < response.getEndIndex()) {
                // 如果主节点最大日志序号小于从节点的最大日志序号, 很可能是之前的主节点在同步日志期间宕机,重新发生了选举, 需要把原主节点的数据截断
                changeState(PushEntryRequest.Type.TRUNCATE);
            } else if (endIndex > response.getEndIndex()) {
                // 如果主节点最大日志序号大于从节点的最大日志序号，从从节点的最大序列号的下一个开始同步
                appendIndex = response.getEndIndex() + 1;
                changeState(PushEntryRequest.Type.APPEND);
            }
        }

        private void doTruncate() {

        }

        private void doCommit() {
            long committedIndex = raftStore.getCommittedIndex();
            if (committedIndex == -1) {
                return;
            }
            lastCommittedIndex = committedIndex;
            CompletableFuture<PushEntryResponse> future = clientProtocol.push(createPushEntryRequest(null, PushEntryRequest.Type.COMMIT, committedIndex));
            future.whenCompleteAsync((response, throwable) -> {
                if (throwable != null) {
                    logger.error("Fail to push commit index {} to peer {}", committedIndex, peerId, throwable);
                    return;
                }
                if (response == null || response.getCode() != ResponseCode.SUCCESS) {
                    logger.warn("Fail to push commit index {} to peer {}", committedIndex, peerId);
                }
            });
        }

        private void doAppend() {
            if (appendIndex == -1) {
                return;
            }
            if (followerEndIndex == raftStore.getEndIndex()) {
                return;
            }
            if (raftStore.getCommittedIndex() != -1 && lastCommittedIndex < raftStore.getCommittedIndex()) {
                doCommit();
                return;
            }
            RaftEntry raftEntry = raftStore.get(appendIndex);
            CompletableFuture<PushEntryResponse> future = clientProtocol.push(createPushEntryRequest(raftEntry, PushEntryRequest.Type.APPEND, appendIndex));
            future.whenComplete((response, throwable) -> {
                if (response.getCode() == ResponseCode.SUCCESS) {
                    updatePeerWaterMark(memberState.getTerm(), peerId, response.getIndex());
                    followerEndIndex = response.getEndIndex();
                    appendIndex++;
                }
            });
        }

        private void changeState(PushEntryRequest.Type type) {
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

    /**
     * 只适用于 follower , 负责处理从 leader 收到的日志
     */
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
                        this.handleAppend(pair.getKey(), pair.getValue());
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
            if (raftStore.getCommittedIndex() != commitIndex) {
                value.complete(createPushEntryResponse(request, ResponseCode.SUCCESS));
                return;
            }
            RaftEntry entry = raftStore.get(commitIndex);
            if (entry == null) {
                value.complete(createPushEntryResponse(request, ResponseCode.INCONSISTENT_STATE));
            } else {
                raftStore.updateCommittedIndex(request.getTerm(), commitIndex);
                value.complete(createPushEntryResponse(request, ResponseCode.SUCCESS));
            }
        }

        private void handleTruncate(long truncateIndex, PushEntryRequest request, CompletableFuture<PushEntryResponse> value) {

        }

        private void handleCompare(long compareIndex, PushEntryRequest request, CompletableFuture<PushEntryResponse> future) {
            RaftEntry entry = raftStore.get(compareIndex);
            if (entry != null && !request.getEntry().equals(entry)) {
                future.complete(createPushEntryResponse(request, ResponseCode.INCONSISTENT_STATE));
            } else {
                future.complete(createPushEntryResponse(request, ResponseCode.SUCCESS));
            }
        }

        private void handleAppend(PushEntryRequest request, CompletableFuture<PushEntryResponse> future) {
            raftStore.append(request.getEntry());
            future.complete(createPushEntryResponse(request, ResponseCode.SUCCESS));
        }

        private PushEntryResponse createPushEntryResponse(PushEntryRequest request, int code) {
            PushEntryResponse response = new PushEntryResponse();
            response.setCode(code);
            response.setBeginIndex(raftStore.getBeginIndex());
            response.setEndIndex(raftStore.getEndIndex());
            if (request != null && request.getEntry() != null) {
                response.setIndex(request.getEntry().getIndex());
            }
            return response;
        }
    }

    /**
     * 只适用于 leader, 负责处理 client 对于 append 的响应
     */
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
                if (pendingAppendResponsesByTerm.isEmpty()) {
                    waitForRunning(1);
                    continue;
                }
                checkPendingAppend(memberState.getTerm());
                ConcurrentMap<String, Long> peerWaterMarks = peerWaterMarksByTerm.get(memberState.getTerm());
                if (peerWaterMarks.isEmpty()) {
                    waitForRunning(1);
                    continue;
                }

                List<Long> waterMarks = peerWaterMarks.values().stream().sorted(Comparator.reverseOrder()).collect(Collectors.toList());
                Long quorumIndex = waterMarks.get(waterMarks.size() / 2);

                CompletableFuture<AppendEntryResponse> future = pendingAppendResponsesByTerm.get(memberState.getTerm()).remove(quorumIndex);
                if (future == null) {
                    waitForRunning(1);
                } else {
                    AppendEntryResponse response = pendingServerResponsesByTerm.get(memberState.getTerm()).remove(quorumIndex);
                    if (response != null) {
                        future.complete(response);
                        raftStore.updateCommittedIndex(memberState.getTerm(), quorumIndex);
                    }
                }
            }
        }

    }
}