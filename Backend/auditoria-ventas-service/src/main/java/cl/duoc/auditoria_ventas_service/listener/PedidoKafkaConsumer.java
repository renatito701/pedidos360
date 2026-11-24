package cl.duoc.auditoria_ventas_service.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PedidoKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(PedidoKafkaConsumer.class);

    @KafkaListener(topics = "pedidos-eventos", groupId = "auditoria-ventas-group")
    public void escucharEventoPedido(String mensajeJson) {
        log.info("==================================================");
        log.info("[AUDITORIA KAFKA] Evento recibido en tiempo real:");
        log.info("{}", mensajeJson);
        log.info("==================================================");
    }
}
