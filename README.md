[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/ItUD98Nn)

# Registro — Consistência de réplicas por ordenação de mensagens (PP 1.4)

Este documento registra o que foi feito para garantir a consistência das réplicas
do sistema P2P, partindo do produto da PP 1.3 (peers + Serviço de Nomes).

## 1. Análise do requisito de consistência

A única operação de atualização do sistema é o `INSERT` de uma transação. As
transações são **imutáveis** e têm `id` próprio; não existe update de linha, delete
nem leitura-modificação-escrita (o `INSERT` nunca consulta o estado antes de gravar).
O estado de cada réplica é, portanto, um **conjunto de registros que só cresce**.

Consequência: do ponto de vista de *igualdade de conteúdo*, os inserts comutam e são
idempotentes (deduplicáveis por `id`), e bastaria consistência forte-eventual. Porém,
o enunciado exige explicitamente implementar o modelo **por meio da ordenação das
mensagens**. Por isso adotamos o modelo mais forte que de fato exercita ordenação e
que entrega uma propriedade trivial de demonstrar: todas as réplicas formam a **mesma
sequência** de operações.

## 2. Modelo escolhido: consistência sequencial (ordem total)

Tratamos o conjunto de peers como uma *replicated state machine*: cada réplica é uma
cópia do mesmo livro-razão, e a condição suficiente para que permaneçam idênticas é
**aplicar as atualizações exatamente na mesma ordem**. Isso é **ordem total**
(*atomic / total-order multicast*), que entrega **consistência sequencial** entre as
réplicas.

Por que não consistência causal: a aplicação não possui dependências causais de nível
de aplicação a preservar; com consistência causal, inserts concorrentes poderiam ser
aplicados em ordens diferentes em réplicas diferentes (os conjuntos convergiriam, mas
a *sequência* não). Ordem total garante sequência idêntica e ainda blinda o sistema
para operações futuras não-comutativas (ex.: impedir saldo negativo).

## 3. Algoritmo: relógio lógico de Lamport + holdback queue + confirmação de todos

Implementação clássica de multicast totalmente ordenado:

1. Cada peer mantém um **relógio lógico de Lamport** (`clock`).
2. Ao originar um `INSERT`, o peer incrementa o relógio, carimba a mensagem com
   `(ts, origin)` e a difunde (`UPDATE`) para todos.
3. Cada peer mantém uma **fila de espera (holdback queue)** ordenada por
   `(ts, origin)`.
4. Ao receber um `UPDATE`, o peer atualiza o relógio (`clock = max(clock, ts)+1`),
   enfileira a mensagem e difunde uma **confirmação** (`ACK`) para todos.
5. A mensagem na **cabeça** da fila é **entregue** (aplicada no banco) somente quando
   foi confirmada por **todos** os membros do grupo. A entrega só ocorre a partir da
   cabeça — nunca fora de ordem.

A ordem total é definida por `(ts, origin)`: o relógio de Lamport dá a ordem causal e
o `origin` (endereço do peer) desempata concorrentes de forma determinística e igual
em todos os nós.

### Premissa de canal FIFO (ponto de atenção importante)

A corretude do algoritmo depende de **canais ponto-a-ponto FIFO e confiáveis**: as
mensagens de um mesmo peer precisam chegar na ordem em que ele as carimbou. Para
garantir isso:

- a camada de rede usa **conexões TCP persistentes** (uma por destino) com
  **framing por tamanho** (prefixo de 4 bytes + JSON), em vez de abrir um socket por
  mensagem;
- o **carimbo-e-envio é atômico**: a mensagem é transmitida ainda sob o lock de
  ordenação, de modo que as mensagens de cada peer saiam na ordem do relógio. O FIFO
  do TCP então preserva essa ordem no fio.

> Esse detalhe foi descoberto em teste: numa versão anterior o carimbo era feito sob o
> lock mas o envio acontecia após soltá-lo, permitindo que o `ACK` de uma mensagem
> nova fosse transmitido antes de uma mensagem mais antiga — quebrando o FIFO e
> causando divergência. Enviar sob o lock corrigiu o problema.

## 4. Tipos de mensagem (entre peers, TCP)

- `UPDATE` — `{msg_id, ts, origin, data}`: operação de atualização a ser ordenada.
- `ACK` — `{msg_id, ts, acker}`: confirmação de recebimento de um `UPDATE`.
- `NEW_PEER` — `{peer}`: anúncio de entrada (mantém "todos conhecem todos").
- `SYNC_REQUEST` / `SYNC_DATA` — transferência do log já entregue (committed) para um
  peer que entra depois.

## 5. Mudanças feitas no `peer.py`

- **Camada de rede reescrita**: conexões persistentes por destino + framing por
  tamanho (`_send_framed` / `_recv_framed`); um *reader* por conexão lê mensagens em
  laço (antes era um `recv` único por socket descartável).
- **Relógio de Lamport** (`clock`, `_tick`, `_update_clock`).
- **Holdback queue** (`queue`), conjunto de confirmações por mensagem (`acks`),
  conjunto de entregues (`delivered`) e contador de ordem global (`seq`).
- **Multicast de ordem total**: `multicast_insert` (originar), `_on_update`,
  `_on_ack`, `_try_deliver` (entrega a cabeça só quando confirmada por todos),
  `_apply` (grava no banco, com dedup por `id`).
- **Esquema do banco** ganhou `seq`, `lamport` e `origin`, tornando a ordem total
  auditável (`ORDER BY seq`).
- **Verificação**: comando `select` imprime o log ordenado e um *hash* do estado;
  comando `hash`/`estado` imprime só o hash. Réplicas consistentes têm hash idêntico.
- A função `notificar_peers` (todos-conhecem-todos) e o `sync_with_oldest` foram
  mantidos, agora usando o framing.

## 6. Como verificar a consistência

Cada peer calcula um **hash SHA-256** sobre a sequência entregue
`(seq, id, sender, receiver, amount, lamport, origin)`. Se todas as réplicas exibem o
**mesmo hash** e o mesmo número de entregues, a ordem total foi mantida. O `select`
mostra o log ordenado por `seq`, idêntico em todos os nós.

## 7. Pressupostos e limitações

- **Membership estável durante a operação**: a regra "confirmado por todos" usa a
  visão de grupo de cada peer. Para a ordem total valer, suba todos os peers e deixe a
  descoberta/notificação assentar **antes** de inserir (no demo, é o que fazemos).
- **Entrada tardia sob quiescência**: um peer que entra sincroniza o log já entregue;
  assuma que não há inserts concorrentes durante o breve instante do join.
- **Bloqueio de cabeça (head-of-line)**: como se espera confirmação de todos, um peer
  caído trava o avanço da fila. É inerente a este modelo; no vídeo, evite derrubar um
  peer no meio de um insert (ou trate isso como ponto de discussão).
- **Desempenho**: enviar sob o lock serializa as transmissões (necessário para o
  FIFO). Para a carga interativa do trabalho é irrelevante; consistência é o objetivo.

## 8. Implantação e demonstração na AWS (6 regiões)

1. `config.py`: `NS_HOST` = IP público da máquina do Serviço de Nomes; `IP_MODE = "aws"`.
2. Libere no Security Group a porta `NS_PORT` (5678) na máquina do Serviço de Nomes e
   a porta TCP de cada peer nas respectivas instâncias.
3. Suba o `name_service.py` em uma instância; e suba os denmais peers, `peer.py`, em outras instâncias.
5. Cenários sugeridos: inserts concorrentes de peers; mensagem atrasada (`tc/netem`);
   cadeia causal; entrada de um 6º peer. E `select`/`hash` em cada
   peer mostrando que os **hashes são iguais** — prova visual da consistência.
