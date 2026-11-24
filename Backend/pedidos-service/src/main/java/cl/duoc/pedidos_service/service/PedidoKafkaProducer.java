package cl.duoc.pedidos_service.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PedidoKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(PedidoKafkaProducer.class);
    public static final String TOPIC_PEDIDOS = "pedidos-eventos";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public PedidoKafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publicarEventoPedido(String pedidoId, String payloadJson) {
        kafkaTemplate.send(TOPIC_PEDIDOS, pedidoId, payloadJson)
            .whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("[KAFKA] Evento publicado en topic={} partition={} offset={} key={}",
                        TOPIC_PEDIDOS,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        pedidoId);
                } else {
                    log.error("[KAFKA] Error al publicar evento para pedido {}: {}", pedidoId, ex.getMessage());
                }
            });
    }
}