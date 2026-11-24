package cl.duoc.kafka_admin_service.controller;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
public class KafkaAdminController {

    @Autowired
    private KafkaAdmin kafkaAdmin;

    @PostMapping("/topics")
    public ResponseEntity<?> crearTopic(@RequestBody Map<String, String> request) {
        String nombreTopic = request.get("nombre");
        if (nombreTopic == null || nombreTopic.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "El nombre del tópico es obligatorio"));
        }

        try (var adminClient = org.apache.kafka.clients.admin.AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            NewTopic newTopic = new NewTopic(nombreTopic, 3, (short) 1);
            adminClient.createTopics(java.util.List.of(newTopic)).all().get();
            return ResponseEntity.ok(Map.of("mensaje", "Tópico creado exitosamente", "topico", nombreTopic));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
}
