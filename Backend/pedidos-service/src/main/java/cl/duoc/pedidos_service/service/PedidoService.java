package cl.duoc.pedidos_service.service;

import cl.duoc.pedidos_service.model.Pedido;
import cl.duoc.pedidos_service.repository.PedidoRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Service
public class PedidoService {

    private final PedidoRepository pedidoRepository;
    private final PedidoProducer pedidoProducer;
    private final PedidoKafkaProducer pedidoKafkaProducer;

    public PedidoService(
            PedidoRepository pedidoRepository,
            PedidoProducer pedidoProducer,
            PedidoKafkaProducer pedidoKafkaProducer) {

        this.pedidoRepository = pedidoRepository;
        this.pedidoProducer = pedidoProducer;
        this.pedidoKafkaProducer = pedidoKafkaProducer;
    }

    public List<Pedido> obtenerTodos() {
        return pedidoRepository.findAll();
    }

    public Optional<Pedido> obtenerPorId(Long id) {
        return pedidoRepository.findById(id);
    }

    public Pedido crear(Pedido pedido) {
        Pedido pedidoGuardado = pedidoRepository.save(pedido);
        pedidoProducer.enviarPedidoCreado(pedidoGuardado.getId());
        pedidoKafkaProducer.publicarEventoPedido(
                String.valueOf(pedidoGuardado.getId()),
                construirEventoJson("PEDIDO_CREADO", pedidoGuardado)
        );
        return pedidoGuardado;
    }

    public Optional<Pedido> actualizar(Long id, Pedido pedidoActualizado) {
        return pedidoRepository.findById(id).map(pedido -> {
            pedido.setCliente(pedidoActualizado.getCliente());
            pedido.setProducto(pedidoActualizado.getProducto());
            pedido.setCantidad(pedidoActualizado.getCantidad());
            pedido.setEstado(pedidoActualizado.getEstado());

            Pedido guardado = pedidoRepository.save(pedido);
            pedidoKafkaProducer.publicarEventoPedido(
                    String.valueOf(guardado.getId()),
                    construirEventoJson("PEDIDO_ACTUALIZADO", guardado)
            );
            return guardado;
        });
    }

    public boolean eliminar(Long id) {
        if (!pedidoRepository.existsById(id)) {
            return false;
        }
        pedidoRepository.deleteById(id);
        return true;
    }

    private String construirEventoJson(String tipoEvento, Pedido p) {
        return String.format(
                "{\"evento\":\"%s\",\"pedidoId\":%d,\"cliente\":\"%s\",\"producto\":\"%s\",\"cantidad\":%d,\"estado\":\"%s\",\"timestamp\":\"%s\"}",
                tipoEvento,
                p.getId(),
                escaparJson(p.getCliente()),
                escaparJson(p.getProducto()),
                p.getCantidad(),
                escaparJson(p.getEstado()),
                Instant.now().toString()
        );
    }

    private String escaparJson(String texto) {
        return texto == null ? "" : texto.replace("\"", "\\\"");
    }
}