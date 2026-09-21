import socket
import threading
import queue
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
        self.thread_relogio = None


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

    while not cliente.evento_sair.is_set():

        item = cliente.fila.get()

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

                enviar(
                    cliente.conexao,
                    cliente.lock_envio,
                    texto_formatado
                )

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


def thread_relogio(cliente, intervalo=60):

    while not cliente.evento_sair.is_set():

        interrompido = cliente.evento_sair.wait(
            timeout=intervalo
        )

        if interrompido:

            break

        try:

            enviar(
                cliente.conexao,
                cliente.lock_envio,
                hora_atual() + "\n"
            )

        except OSError:

            cliente.evento_sair.set()

            break


#FASE 2 (TUDO Q O SERVIDOR PRECISA FAZER PARA ATENDER 1 CLIENTE)

def atender_cliente(cliente):

    conexao = cliente.conexao
    endereco = cliente.endereco

    print(f"Cliente conectado: {endereco}")

    # Adiciona o cliente à lista global

    with lock_clientes:

        clientes.append(cliente)

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

    cliente.thread_relogio = threading.Thread(
        target=thread_relogio,
        args=(cliente,)
    )

    # Inicia as threads

    cliente.thread_recepcao.start()
    cliente.thread_processamento.start()
    cliente.thread_relogio.start()

    # Aguarda as threads terminarem

    cliente.thread_recepcao.join()
    cliente.thread_processamento.join()
    cliente.thread_relogio.join()

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

    servidor = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    servidor.bind((HOST, PORT))

    servidor.listen()

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
                args=(cliente,)
            )

            thread_cliente.start()

    except KeyboardInterrupt:

        print("\nServidor encerrando...")

    finally:

        servidor.close()


if __name__ == "__main__":

    main()