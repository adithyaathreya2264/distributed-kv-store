package com.dkv.server;

import com.dkv.kv.proto.KvProtos;
import com.dkv.raft.StateMachine;
import com.dkv.storage.LSMStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KVStateMachine implements StateMachine {
    private static final Logger logger = LoggerFactory.getLogger(KVStateMachine.class);
    private final LSMStorageEngine storageEngine;

    public KVStateMachine(LSMStorageEngine storageEngine) {
        this.storageEngine = storageEngine;
    }

    @Override
    public void apply(byte[] command) {
        try {
            KvProtos.KVRequest request = KvProtos.KVRequest.parseFrom(command);
            switch (request.getType()) {
                case PUT:
                    storageEngine.put(request.getKey(), request.getValue());
                    logger.info("Applied PUT {}={}", request.getKey(), request.getValue());
                    break;
                case DELETE:
                    storageEngine.put(request.getKey(), null);
                    logger.info("Applied DELETE {}", request.getKey());
                    break;
                case GET:
                    break;
                default:
                    logger.warn("Unknown message type: {}", request.getType());
            }
        } catch (IOException e) {
            logger.error("Failed to apply command", e);
        }
    }
}
