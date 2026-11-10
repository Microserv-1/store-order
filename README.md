# Store Order Service

Serviço de pedidos (`POST /orders`, `GET /orders`, `GET /orders/{id}`) em Python/FastAPI com isolamento por conta, armazenamento em **USD**, conversão sob demanda via `store-exchange` e **Mensageria Assíncrona com RabbitMQ (Outbox + Consumidor Idempotente)** como segundo tratamento de gargalo da Pessoa B.

## Endpoints

| Método | Rota | Autenticação | Resposta |
|---|---|---|---|
| `POST` | `/orders` | Bearer JWT | `201 Created`: cria pedido em USD validando produtos em `store-product` (`400` se produto inexistente) |
| `GET` | `/orders` | Bearer JWT | `200 OK`: lista apenas os pedidos da conta autenticada |
| `GET` | `/orders/{id}` | Bearer JWT | `200 OK`: detalhe do pedido (`404` se pertencer a outra conta; `?currency=BRL` converte via `store-exchange`; `422` se moeda não suportada) |
| `GET` | `/orders/{id}/notifications` | Bearer JWT | `200 OK`: estado do processamento assíncrono de notificação do pedido |
| `POST` | `/orders/{id}/events/replay` | Bearer JWT | Reenvia evento `order.created` para demonstrar idempotência do consumidor |
| `GET` | `/orders/events/summary` | Bearer JWT | Resumo de eventos publicados, consumidos, pendentes e duplicatas descartadas |
| `POST` | `/orders/events/consumer` | Bearer JWT | Pausa/retoma o consumidor assíncrono para demonstrar retomada após indisponibilidade |
| `GET` | `/health-check` | Público | Saúde do processo |
| `GET` | `/ready` | Público | Prontidão do banco e broker RabbitMQ |
| `GET` | `/metrics` | Público (interno) | Métricas Prometheus |

## Regra de Conversão Monetária e Arredondamento

- Todos os valores de pedidos e itens são persistidos no PostgreSQL em **USD** (`Numeric(12, 2)`).
- Quando `?currency=<MOEDA>` é informado em `GET /orders/{id}`, o serviço consulta `GET /exchanges/USD/<MOEDA>` no `store-exchange`, utiliza a taxa **`sell`** (cotação de venda aplicável à conversão de compra pelo cliente) e arredonda cada item para 2 casas decimais (`ROUND_HALF_UP`), calculando o total convertido como a soma dos itens convertidos sem alterar o registro em USD no banco.
