import socket
import threading

HOST = "127.0.0.1"
PORT = 5051
ENCODING = "utf-8"
BUFFER_SIZE = 1024

sair = threading.Event()
buffer_recv = ""


def receber_linha(sock):
    """Le do socket ate ter uma linha completa (terminada em \\n) no buffer.
    Retorna None se o servidor fechou a conexao (recv == b"")."""
    global buffer_recv
    while "\n" not in buffer_recv:
        dados = sock.recv(BUFFER_SIZE)
        if not dados:
            return None
        buffer_recv += dados.decode(ENCODING, errors="replace")
    linha, buffer_recv = buffer_recv.split("\n", 1)
    return linha


def thread_enviar(sock):
    while not sair.is_set():
        try:
            texto = input()
        except (EOFError, KeyboardInterrupt):
            texto = ":quit"

        if sair.is_set():
            break

        try:
            sock.sendall((texto + "\n").encode(ENCODING))
        except OSError as erro:
            print(f"Erro ao enviar mensagem: {erro}")
            sair.set()
            break

        if texto.strip() == ":quit":
            sair.set()
            break


def thread_receber(sock):
    while not sair.is_set():
        try:
            linha = receber_linha(sock)
        except OSError as erro:
            if not sair.is_set():
                print(f"Erro ao receber dados: {erro}")
            sair.set()
            break

        if linha is None:
            print("Conexao encerrada pelo servidor.")
            sair.set()
            break

        print(linha)


def main():
    global buffer_recv

    cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        cliente.connect((HOST, PORT))
    except OSError as erro:
        print(f"Nao foi possivel conectar ao servidor {HOST}:{PORT}: {erro}")
        return

    print("Conectado ao servidor!")

    try:
        boas_vindas = receber_linha(cliente)
    except OSError as erro:
        print(f"Erro ao receber mensagem de boas-vindas: {erro}")
        cliente.close()
        return

    if boas_vindas is None:
        print("Servidor encerrou a conexao antes de enviar boas-vindas.")
        cliente.close()
        return

    print(boas_vindas)

    t1 = threading.Thread(target=thread_enviar, args=(cliente,), daemon=True)
    t2 = threading.Thread(target=thread_receber, args=(cliente,), daemon=True)
    t1.start()
    t2.start()

    sair.wait()

    try:
        cliente.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    cliente.close()
    print("Conexao encerrada.")


if __name__ == "__main__":
    main()
