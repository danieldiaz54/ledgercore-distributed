# PRODUCT.md — `ledgercore-distributed`

## 1. Propósito en una frase
**Billetera digital y transferencias seguras**: consulta tu saldo, envía dinero a otro contacto en segundos y comprueba que hacer doble clic o perder conexión jamás cobra dos veces.

## 2. Regla de 2 Capas (Usuario primero, Arquitectura bajo demanda)
- **Capa 1 (`Operate` Institutional Pine Banking):** Tarjeta principal de saldo y selector de cuentas, formulario directo **"Enviar dinero"** (`$ USD`), dos demostraciones interactivas cotidianas en 1 clic (**`"Probar doble clic"`** y **`"Simular fallo de red"`**), e historial limpio de movimientos recientes.
- **Capa 2 (Cajón `"Arquitectura"` opcional):** Auditoría de partida doble Zero-Sum (`imbalance_cents = 0`), estado de Sagas, Transactional Outbox + botón `"Despachar Outbox"`, eventos Kafka y Dead-Letter Queue.
