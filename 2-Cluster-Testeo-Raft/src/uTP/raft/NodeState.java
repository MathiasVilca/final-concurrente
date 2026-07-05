package uTP.raft;

public enum NodeState {
    FOLLOWER,   // Seguidor: Rol inicial, solo escucha y obedece al líder.
    CANDIDATE,  // Candidato: Su tiempo se agotó, pide votos para ser el nuevo líder.
    LEADER      // Líder: Gobierna el clúster, envía latidos (Heartbeats) y recibe las peticiones.
}