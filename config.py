# ============================================================
#  Unico endereco estatico do sistema: o do Servico de Nomes.
#  Todo peer le isto para o bootstrap (bind / register / discover).
#  Os enderecos dos PEERS nao ficam aqui - sao descobertos em tempo
#  de execucao e registrados no Servico de Nomes.
# ============================================================

# Local: "127.0.0.1"
# AWS:   IP publico (ou DNS) da maquina que roda o name_service.py
NS_HOST = "127.0.0.1"
NS_PORT = 5555

# Como cada peer descobre o PROPRIO IP para se anunciar.
# Nao e um endereco - e apenas um modo de operacao:
#   "local" -> usa 127.0.0.1 (todos os peers na mesma maquina)
#   "aws"   -> detecta o IP publico da instancia em tempo de execucao
IP_MODE = "local"
