import socket
import threading
import queue
import sys
import time
from datetime import datetime
HOST = "127.0.0.1"
PORT = 5051

#CRIANDO ESTRUTURA PARA O CLIENTE, PARA PODER TER VARIAS THREADS E DADOS EXCLUSIVOS PARA CADA CLIENTE

class Cliente:

    def __init__(self, conexao, endereco):

        self.conexao = conexao
        self.endereco = endereco

        # Nome inicial do usuário
        self.nome = f"{endereco[0]}:{endereco[1]}"

        # Estruturas exclusivas deste cliente
        self.fila = queue.Queue()
        self.evento_sair = threading.Event()
        self.lock_envio = threading.Lock()

        # Threads deste cliente
        self.thread_recepcao = None
        self.thread_processamento = None


clientes = []

lock_clientes = threading.Lock()

#colocamos lock para n dar race condition, evitando q as threads acessem a lista ao mesmo tempo


def hora_atual():
    return datetime.now().strftime("%H:%M:%S")


def enviar(conexao, lock, texto):
    with lock:
        conexao.sendall(texto.encode())


def thread_recepcao(cliente):
    conexao = cliente.conexao
    while not cliente.evento_sair.is_set():

        try:
            dados = conexao.recv(1024)

        except OSError:

            cliente.fila.put({
                "tipo": "comando_quit",
                "conteudo": "",
                "cliente": cliente
            })

            break

        if not dados:

            cliente.fila.put({
                "tipo": "comando_quit",
                "conteudo": "",
                "cliente": cliente
            })

            break

        texto = dados.decode(
            errors="replace"
        ).strip()

        if texto.startswith(":nome "):

            novo_nome = texto[len(":nome "):].strip()

            cliente.fila.put({
                "tipo": "comando_nome",
                "conteudo": novo_nome,
                "cliente": cliente
            })

        elif texto.startswith(":quit"):

            cliente.fila.put({
                "tipo": "comando_quit",
                "conteudo": "",
                "cliente": cliente
            })

            break

        else:

            cliente.fila.put({
                "tipo": "mensagem",
                "conteudo": texto,
                "cliente": cliente
            })


def thread_processamento(cliente):

    ultimo_relogio = time.monotonic()

    while not cliente.evento_sair.is_set():

        try:

            item = cliente.fila.get(timeout=1)

        except queue.Empty:

            item = None

        # Envia a hora a cada 60 segundos, mesmo sem mensagens

        if time.monotonic() - ultimo_relogio >= 60:

            ultimo_relogio = time.monotonic()

            try:

                enviar(
                    cliente.conexao,
                    cliente.lock_envio,
                    hora_atual() + "\n"
                )

            except OSError:

                cliente.evento_sair.set()

                break

        if item is None:

            continue

        try:

            if item["tipo"] == "mensagem":

                texto_formatado = (
                    f"{cliente.nome} "
                    f"({hora_atual()}): "
                    f"{item['conteudo']}\n"
                )

                eco = (
                    f"Voce digitou: "
                    f"{item['conteudo']}\n"
                )

                with lock_clientes:

                    destinos = list(clientes)

                for destino in destinos:

                    if destino is cliente:

                        continue

                    try:

                        enviar(
                            destino.conexao,
                            destino.lock_envio,
                            texto_formatado
                        )

                    except OSError:

                        pass

                enviar(
                    cliente.conexao,
                    cliente.lock_envio,
                    eco
                )

            elif item["tipo"] == "comando_nome":

                cliente.nome = item["conteudo"]

                enviar(
                    cliente.conexao,
                    cliente.lock_envio,
                    f"Nome alterado para: "
                    f"{cliente.nome}\n"
                )

            elif item["tipo"] == "comando_quit":

                try:

                    enviar(
                        cliente.conexao,
                        cliente.lock_envio,
                        "Encerrando conexao...\n"
                    )

                except OSError:

                    pass

                cliente.evento_sair.set()

                cliente.conexao.close()

                break

        except OSError:

            cliente.evento_sair.set()

            break


#FASE 2 (TUDO Q O SERVIDOR PRECISA FAZER PARA ATENDER 1 CLIENTE)

def atender_cliente(cliente, max_clientes):

    conexao = cliente.conexao
    endereco = cliente.endereco

    print(f"Cliente conectado: {endereco}")

    # Verifica o limite e adiciona o cliente à lista global (atomicamente)

    with lock_clientes:

        cheio = len(clientes) >= max_clientes

        if not cheio:

            clientes.append(cliente)

    # Limite atingido: avisa, fecha a conexão e encerra este worker

    if cheio:

        try:

            enviar(
                conexao,
                cliente.lock_envio,
                "Limite de clientes atingido\n"
            )

        except OSError:

            pass

        conexao.close()

        return

    # Envia mensagem inicial

    try:

        enviar(
            conexao,
            cliente.lock_envio,
            f"{hora_atual()}: CONECTADO!!\n"
        )

    except OSError:

        cliente.evento_sair.set()

    # Cria as threads deste cliente

    cliente.thread_recepcao = threading.Thread(
        target=thread_recepcao,
        args=(cliente,)
    )

    cliente.thread_processamento = threading.Thread(
        target=thread_processamento,
        args=(cliente,)
    )

    # Inicia as threads

    cliente.thread_recepcao.start()
    cliente.thread_processamento.start()

    # Aguarda as threads terminarem

    cliente.thread_recepcao.join()
    cliente.thread_processamento.join()

    # Remove o cliente da lista

    with lock_clientes:

        if cliente in clientes:

            clientes.remove(cliente)

    # Fecha o socket

    try:

        cliente.conexao.close()

    except OSError:

        pass

    print(f"Cliente desconectado: {endereco}")


def main():

    try:

        max_clientes = int(sys.argv[1])

    except (IndexError, ValueError):

        print("Uso: python3 server.py <max_clientes>")
        return

    servidor = None

    try:

        servidor = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        servidor.bind((HOST, PORT))

        servidor.listen()

    except OSError:

        print("Nao foi possivel iniciar o servidor")

        if servidor is not None:
            servidor.close()

        return

    print("Servidor iniciado")

    print(f"Aguardando conexoes em {HOST}:{PORT}...")

    try:

        while True:

            conexao, endereco = servidor.accept()

            cliente = Cliente(
                conexao,
                endereco
            )

            thread_cliente = threading.Thread(
                target=atender_cliente,
                args=(cliente, max_clientes)
            )

            thread_cliente.start()

    except KeyboardInterrupt:

        print("\nServidor encerrando...")

    finally:

        servidor.close()


if __name__ == "__main__":

    main()