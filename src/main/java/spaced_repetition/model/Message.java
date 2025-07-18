package spaced_repetition.model;

public record Message(String id, String question, int correctAnswers, String answer) {
    public Message up() {
        return new Message(id(), question, correctAnswers() + 1, answer());
    }
}
