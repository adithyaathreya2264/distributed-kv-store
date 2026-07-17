package com.dkv.server;

import com.dkv.kv.proto.KvProtos.*;
import com.dkv.raft.RaftNode;
import com.dkv.raft.RaftState;
import com.dkv.storage.LSMStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KVRequestHandler {
    private static final Logger logger = LoggerFactory.getLogger(KVRequestHandler.class);
    private final RaftNode raftNode;
    private final LSMStorageEngine storageEngine;

    public KVRequestHandler(RaftNode raftNode, LSMStorageEngine storageEngine) {
        this.raftNode = raftNode;
        this.storageEngine = storageEngine;
    }

    public KVResponse handle(KVRequest request) {
        KVResponse.Builder response = KVResponse.newBuilder();

        if (request.getType() == KVRequest.CommandType.GET) {
            try {
                if (raftNode.getState() != RaftState.LEADER) {
                    return response.setSuccess(false).setMessage("Not Leader").build();
                }

                String val = storageEngine.get(request.getKey());
                if (val != null) {
                    return response.setSuccess(true).setValue(val).build();
                } else {
                    return response.setSuccess(false).setMessage("Key not found").build();
                }
            } catch (IOException e) {
                logger.error("Error reading from storage", e);
                return response.setSuccess(false).setMessage("IO Error").build();
            }
        } else {
            // PUT / DELETE
            if (raftNode.getState() != RaftState.LEADER) {
                return response.setSuccess(false).setMessage("Not Leader").build();
            }

            // Serialize request to bytes and propose to Raft
            byte[] command = request.toByteArray();
            boolean proposed = raftNode.propose(command);

            if (proposed) {
                return response.setSuccess(true).setMessage("Command accepted for replication").build();
            } else {
                return response.setSuccess(false).setMessage("Failed to propose command").build();
            }
        }
    }
}
