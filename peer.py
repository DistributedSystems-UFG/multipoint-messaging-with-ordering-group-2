import socket
import threading
import json
import struct
import uuid
import hashlib
import duckdb
from datetime import datetime
from config import *
import zmq


def get_public_ip():
    # O modo vem do config.py (IP_MODE). O config so manda no MODO,
    # nunca no endereco do peer: o IP e detectado aqui em tempo de
    # execucao e depois registrado no Servico de Nomes via bind.
    if IP_MODE == "local":
        return "127.0.0.1"
    from requests import get
    return get("https://api.ipify.org").content.decode("utf8")


# ---------------------------------------------------------------------------
# Framing de mensagens: prefixo de 4 bytes (tamanho) + JSON em UTF-8.
# Necessario porque as conexoes agora sao PERSISTENTES e varias mensagens
# trafegam na mesma conexao - um recv() unico nao basta para delimita-las.
# ---------------------------------------------------------------------------
def _send_framed(sock, obj):
    data = json.dumps(obj).encode("utf-8")
    sock.sendall(struct.pack("!I", len(data)) + data)


def _recv_exactly(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def _recv_framed(sock):
    header = _recv_exactly(sock, 4)
    if header is None:
        return None
    (length,) = struct.unpack("!I", header)
    payload = _recv_exactly(sock, length)
    if payload is None:
        return None
    return json.loads(payload.decode("utf-8"))


class Peer:
    def __init__(self, port=None):
        self.host = "0.0.0.0"
        self.port = port if port is not None else int(input("Porta deste peer: "))
        self.name = "peer_" + str(self.port) + "_" + str(uuid.uuid4())[:4]
        self.address = f"{get_public_ip()}:{self.port}"
        self.peers = set()
        self.oldest_peer = None

        # ---- estado da ordenacao total (consistencia sequencial) ----
        self.lock = threading.RLock()   # protege relogio, fila, acks, seq e o banco
        self.clock = 0                  # relogio logico de Lamport
        self.queue = {}                 # holdback queue: msg_id -> mensagem UPDATE pendente
        self.acks = {}                  # msg_id -> conjunto de enderecos que confirmaram
        self.delivered = set()          # msg_ids ja entregues (deduplicacao)
        self.seq = 0                    # ordem global de entrega (identica em todas as replicas)

        # ---- conexoes de saida persistentes (garantem canal FIFO por TCP) ----
        self.out_conns = {}             # address -> socket
        self.io_lock = threading.Lock()

        self.conn = duckdb.connect(f"peer_{self.port}.db")
        self._init_database()

    def _init_database(self):
        # seq, lamport e origin tornam a ordem total visivel e auditavel
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            seq INTEGER,
            id INTEGER,
            sender VARCHAR,
            receiver VARCHAR,
            amount DOUBLE,
            lamport INTEGER,
            origin VARCHAR,
            timestamp TIMESTAMP
        )
        """)

    # ===================== Servico de Nomes (ZeroMQ) =====================
    def conectar_servico_nomes(self):
        context = zmq.Context()
        s = context.socket(zmq.REQ)
        s.connect(f"tcp://{NS_HOST}:{NS_PORT}")

        s.send_json({"op": "bind", "name": self.name, "address": self.address})
        s.recv_json()

        s.send_json({"op": "register", "name": self.name, "type": "peer"})
        s.recv_json()

        s.send_json({"op": "discover", "type": "peer"})
        resposta = s.recv_json()
        lista = resposta.get("result", [])

        for p in lista:
            if p["name"] != self.name:
                self.peers.add(p["address"])

        if len(lista) > 1 and lista[0]["name"] != self.name:
            self.oldest_peer = lista[0]["address"]
        else:
            self.oldest_peer = None

        print(f"[PEERS CONHECIDOS] {self.peers}")
        print(f"[NO MAIS VELHO] {self.oldest_peer}")

    def notificar_peers(self):
        # avisa os peers antigos da entrada deste no (mantem "todos conhecem todos")
        msg = {"type": "NEW_PEER", "peer": self.address}
        for address in list(self.peers):
            self.send_to(address, msg)

    # ===================== Relogio logico de Lamport =====================
    def _tick(self):
        # evento interno / envio de mensagem
        self.clock += 1
        return self.clock

    def _update_clock(self, ts):
        # recepcao de mensagem
        self.clock = max(self.clock, ts) + 1
        return self.clock

    # ===================== Camada de rede (persistente, FIFO) =====================
    def _get_conn(self, address):
        sock = self.out_conns.get(address)
        if sock is not None:
            return sock
        host, port = address.split(":")
        sock = socket.socket()
        sock.settimeout(5)
        sock.connect((host, int(port)))
        sock.settimeout(None)
        self.out_conns[address] = sock
        return sock

    def _drop_conn(self, address):
        sock = self.out_conns.pop(address, None)
        if sock:
            try:
                sock.close()
            except Exception:
                pass

    def send_to(self, address, obj):
        # uma conexao persistente por destino; reconecta uma vez em caso de falha
        with self.io_lock:
            for tentativa in (1, 2):
                try:
                    sock = self._get_conn(address)
                    _send_framed(sock, obj)
                    return True
                except Exception:
                    self._drop_conn(address)
            return False

    def broadcast(self, obj):
        for address in list(self.peers):
            self.send_to(address, obj)

    # ===================== Multicast de ordem total =====================
    def multicast_insert(self, tx):
        # originar um UPDATE: carimba com Lamport, enfileira e DIFUNDE sob o lock.
        # Enviar sob o lock garante que as mensagens deste peer saiam na ordem do
        # relogio (carimbar-e-enviar atomico). Com o FIFO do TCP, cada destino
        # recebe as mensagens deste peer na ordem de Lamport - premissa exigida
        # pelo algoritmo de ordem total.
        with self.lock:
            ts = self._tick()
            msg_id = str(uuid.uuid4())
            msg = {"type": "UPDATE", "msg_id": msg_id, "ts": ts,
                   "origin": self.address, "data": tx}
            self.queue[msg_id] = msg
            self.acks[msg_id] = {self.address}   # a origem ja conta como confirmacao
            self.broadcast(msg)
            self._try_deliver()

    def _on_update(self, msg):
        with self.lock:
            self._update_clock(msg["ts"])
            mid = msg["msg_id"]
            if mid not in self.delivered and mid not in self.queue:
                self.queue[mid] = msg
                conjunto = self.acks.setdefault(mid, set())
                # a origem conta como confirmacao; este peer tambem confirma
                conjunto.update({msg["origin"], self.address})
                # ts da confirmacao e > ts do update (garante a ordem total via canal FIFO).
                # O ACK e enviado SOB O LOCK para preservar a ordem de envio deste peer.
                ack = {"type": "ACK", "msg_id": mid, "ts": self.clock, "acker": self.address}
                self.broadcast(ack)
            self._try_deliver()

    def _on_ack(self, msg):
        with self.lock:
            self._update_clock(msg["ts"])
            mid = msg["msg_id"]
            if mid in self.delivered:
                return
            self.acks.setdefault(mid, set()).add(msg["acker"])
            self._try_deliver()

    def _group(self):
        return {self.address} | self.peers

    def _try_deliver(self):
        # entrega (aplica no banco) apenas a CABECA da fila, e somente quando
        # ela ja foi confirmada por TODOS os membros do grupo. Isso garante
        # que todas as replicas apliquem na mesma ordem (ts, origin).
        while self.queue:
            mid = min(self.queue,
                      key=lambda k: (self.queue[k]["ts"], self.queue[k]["origin"]))
            if self.acks.get(mid, set()) >= self._group():
                msg = self.queue.pop(mid)
                self.acks.pop(mid, None)
                self.delivered.add(mid)
                self._apply(msg)
            else:
                break

    def _apply(self, msg):
        d = msg["data"]
        existe = self.conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE id = ?", (d["id"],)
        ).fetchone()[0]
        if existe:
            return
        self.seq += 1
        self.conn.execute(
            "INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (self.seq, d["id"], d["sender"], d["receiver"], d["amount"],
             msg["ts"], msg["origin"], datetime.now())
        )
        print(f"[ENTREGUE seq={self.seq} ts={msg['ts']} origin={msg['origin']}] {d}")

    # ===================== Servidor TCP (leitores persistentes) =====================
    def server(self):
        s = socket.socket()
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.host, self.port))
        s.listen()
        print(f"[LISTENING] {self.port}")
        while True:
            conn_sock, _ = s.accept()
            threading.Thread(target=self._reader, args=(conn_sock,), daemon=True).start()

    def _reader(self, conn_sock):
        try:
            while True:
                msg = _recv_framed(conn_sock)
                if msg is None:
                    break
                self._dispatch(msg, conn_sock)
        except Exception:
            pass
        finally:
            try:
                conn_sock.close()
            except Exception:
                pass

    def _dispatch(self, msg, conn_sock):
        t = msg.get("type")
        if t == "UPDATE":
            self._on_update(msg)
        elif t == "ACK":
            self._on_ack(msg)
        elif t == "NEW_PEER":
            self._on_new_peer(msg)
        elif t == "SYNC_REQUEST":
            self._on_sync_request(conn_sock)

    def _on_new_peer(self, msg):
        new_peer = msg["peer"]
        if new_peer != self.address:
            with self.lock:
                self.peers.add(new_peer)
            print(f"[NOVO PEER] {new_peer}")

    def _on_sync_request(self, conn_sock):
        with self.lock:
            rows = self.conn.execute(
                "SELECT seq,id,sender,receiver,amount,lamport,origin,timestamp "
                "FROM transactions ORDER BY seq"
            ).fetchall()
            clock = self.clock
            seq = self.seq
        data = [[r[0], r[1], r[2], r[3], r[4], r[5], r[6],
                 r[7].isoformat() if r[7] else None] for r in rows]
        _send_framed(conn_sock, {"type": "SYNC_DATA", "data": data,
                                 "clock": clock, "seq": seq})

    def sync_with_oldest(self):
        if not self.oldest_peer:
            print("[SYNC] primeiro no, nada a sincronizar")
            return
        try:
            host, port = self.oldest_peer.split(":")
            s = socket.socket()
            s.settimeout(10)
            s.connect((host, int(port)))
            _send_framed(s, {"type": "SYNC_REQUEST"})
            resp = _recv_framed(s)
            s.close()
            if resp and resp.get("type") == "SYNC_DATA":
                with self.lock:
                    for row in resp["data"]:
                        self.conn.execute(
                            "INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (row[0], row[1], row[2], row[3], row[4], row[5], row[6],
                             datetime.fromisoformat(row[7]) if row[7] else datetime.now())
                        )
                    self.seq = max(self.seq, resp.get("seq", 0))
                    self.clock = max(self.clock, resp.get("clock", 0))
                print(f"[SYNC] {len(resp['data'])} registros recebidos "
                      f"(seq->{self.seq}, clock->{self.clock})")
        except Exception as e:
            print("[SYNC ERROR]", e)

    # ===================== Verificacao de consistencia =====================
    def _state_hash(self):
        rows = self.conn.execute(
            "SELECT seq,id,sender,receiver,amount,lamport,origin "
            "FROM transactions ORDER BY seq"
        ).fetchall()
        h = hashlib.sha256()
        for r in rows:
            h.update(f"{r[0]}|{r[1]}|{r[2]}|{r[3]}|{r[4]}|{r[5]}|{r[6]};".encode())
        return h.hexdigest()[:12]

    def _print_log(self):
        rows = self.conn.execute(
            "SELECT seq,id,sender,receiver,amount,lamport,origin "
            "FROM transactions ORDER BY seq"
        ).fetchall()
        print("[LOG ORDENADO]")
        for r in rows:
            print(f"  seq={r[0]} id={r[1]} {r[2]}->{r[3]} {r[4]} "
                  f"(ts={r[5]}, origin={r[6]})")
        print(f"  hash={self._state_hash()}  entregues={self.seq}")

    # ===================== Cliente interativo =====================
    def client(self):
        while True:
            cmd = input(">> ").strip()
            if cmd.startswith("insert"):
                try:
                    _, id, sender, receiver, amount = cmd.split()
                    tx = {"id": int(id), "sender": sender,
                          "receiver": receiver, "amount": float(amount)}
                    self.multicast_insert(tx)
                except ValueError:
                    print("uso: insert <id> <remetente> <destinatario> <valor>")
            elif cmd == "select":
                self._print_log()
            elif cmd in ("hash", "estado"):
                print(f"[ESTADO] entregues={self.seq} hash={self._state_hash()}")
            elif cmd == "peers":
                print(f"[PEERS] {self.peers}")
            elif cmd in ("quit", "exit"):
                break

    def run(self):
        self.conectar_servico_nomes()
        threading.Thread(target=self.server, daemon=True).start()
        self.notificar_peers()
        self.sync_with_oldest()
        self.client()


if __name__ == "__main__":
    peer = Peer()
    peer.run()
