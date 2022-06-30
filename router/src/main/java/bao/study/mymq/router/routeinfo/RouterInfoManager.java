package bao.study.mymq.router.routeinfo;

import bao.study.mymq.common.route.StoreHeader;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author baoyh
 * @since 2022/6/17 17:14
 */
public class RouterInfoManager {

    private final Map<String/* storeName */, StoreHeader> storeTable = new ConcurrentHashMap<>();

    private final Map<String/* clusterName */, Set<String/* storeName */>> clusterTable = new ConcurrentHashMap<>();

    public void registerStore(StoreHeader storeHeader) {
        storeTable.putIfAbsent(storeHeader.getStoreName(), storeHeader);
        Set<String> storeNameSet = clusterTable.getOrDefault(storeHeader.getClusterName(), new HashSet<>());
        storeNameSet.add(storeHeader.getStoreName());
    }
}
