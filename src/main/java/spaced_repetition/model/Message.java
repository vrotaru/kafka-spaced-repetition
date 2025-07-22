package spaced_repetition.model;

import java.util.UUID;

public record Message(String id, String question, int correctAnswers, String answer) {
    
    public Message(String question, String answer) {
        this(UUID.randomUUID().toString(), question, 0, answer);
    }
    
    public Message up() {
        return new Message(id(), question, correctAnswers() + 1, answer());
    }
}
