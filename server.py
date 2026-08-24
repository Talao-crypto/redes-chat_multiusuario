import socket
import threading
import queue
from datetime import datetime

HOST = "127.0.0.1"  # definindo a porta e o endereço
PORT = 5051


def hora_atual():
    return datetime.now().strftime("%H:%M:%S")


def enviar(conexao, lock, texto):
    with lock:
        conexao.sendall(texto.encode())


def thread_recepcao(conexao, endereco, fila, evento_sair):
    nome_usuario = f"{endereco[0]}:{endereco[1]}"  # nome padrão

    while not evento_sair.is_set():
        try:
            dados = conexao.recv(1024)
        except OSError:
            fila.put({"tipo": "comando_quit", "conteudo": "", "usuario": nome_usuario})
            break

        if not dados:
            fila.put({"tipo": "comando_quit", "conteudo": "", "usuario": nome_usuario})
            break

        texto = dados.decode(errors="replace").strip()

        if texto.startswith(":nome "):
            nome_usuario = texto[len(":nome "):].strip()
            fila.put({"tipo": "comando_nome", "conteudo": nome_usuario, "usuario": nome_usuario})
        elif texto.startswith(":quit"):
            fila.put({"tipo": "comando_quit", "conteudo": "", "usuario": nome_usuario})
            break
        else:
            fila.put({"tipo": "mensagem", "conteudo": texto, "usuario": nome_usuario})


def thread_processamento(conexao, fila, evento_sair, lock_envio):
    while not evento_sair.is_set():
        item = fila.get()  # bloqueia até ter algo

        try:
            if item["tipo"] == "mensagem":
                texto_formatado = f"{item['usuario']} ({hora_atual()}): {item['conteudo']}\n"
                eco = f"Voce digitou: {item['conteudo']}\n"
                enviar(conexao, lock_envio, texto_formatado)
                enviar(conexao, lock_envio, eco)

            elif item["tipo"] == "comando_nome":
                enviar(conexao, lock_envio, f"Nome alterado para: {item['conteudo']}\n")

            elif item["tipo"] == "comando_quit":
                try:
                    enviar(conexao, lock_envio, "Encerrando conexao...\n")
                except OSError:
                    pass
                evento_sair.set()
                conexao.close()
                break
        except OSError:
            evento_sair.set()
            break


def thread_relogio(conexao, evento_sair, lock_envio, intervalo=60):
    while not evento_sair.is_set():
        interrompido = evento_sair.wait(timeout=intervalo)
        if interrompido:
            break
        try:
            enviar(conexao, lock_envio, hora_atual() + "\n")
        except OSError:
            evento_sair.set()
            break


def main():
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.bind((HOST, PORT))
    servidor.listen(1)

    print("Servidor iniciado")
    print("Aguardando conexão...")

    conexao, endereco = servidor.accept()
    print(f"Cliente conectado: {endereco}")

    enviar(conexao, threading.Lock(), f"{hora_atual()}: CONECTADO!!\n")

    fila = queue.Queue()
    evento_sair = threading.Event()
    lock_envio = threading.Lock()

    t1 = threading.Thread(target=thread_recepcao, args=(conexao, endereco, fila, evento_sair))
    t2 = threading.Thread(target=thread_processamento, args=(conexao, fila, evento_sair, lock_envio))
    t3 = threading.Thread(target=thread_relogio, args=(conexao, evento_sair, lock_envio))

    t1.start()
    t2.start()
    t3.start()

    t1.join()
    t2.join()
    t3.join()

    conexao.close()
    servidor.close()
    print("Servidor encerrado")


if __name__ == "__main__":
    main()
