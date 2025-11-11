package org.apache.shardingsphere.example.proxy.cdc.service;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * WebSocket Service
 */
@Slf4j
@Service
public class WebSocketService {

    private final Set<WebSocketSession> sessions = new CopyOnWriteArraySet<>();
    private final Gson gson = new Gson();

    public void addSession(WebSocketSession session) {
        sessions.add(session);
        log.info("Added session: {}. Total sessions: {}", session.getId(), sessions.size());
    }

    public void removeSession(WebSocketSession session) {
        sessions.remove(session);
        log.info("Removed session: {}. Total sessions: {}", session.getId(), sessions.size());
    }

    public void broadcast(String type, Object payload) {
        Map<String, Object> message = new HashMap<>();
        message.put("type", type);
        message.put("payload", payload);
        String json = gson.toJson(message);
        
        sessions.forEach(session -> {
            try {
                if (session.isOpen()) {
                    session.sendMessage(new TextMessage(json));
                }
            } catch (IOException e) {
                log.error("Error broadcasting message to session: {}", session.getId(), e);
            }
        });
    }
}
